/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hyperledger.network.server

import java.net.{InetAddress, InetSocketAddress}
import java.time.Clock

import akka.actor._
import akka.io.Inet
import akka.pattern._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Tcp}
import akka.util.Timeout
import org.hyperledger.common.{Header, Transaction}
import org.hyperledger.network.Messages._
import org.hyperledger.network.RejectCode.REJECT_OBSOLETE
import org.hyperledger.network.flows._
import org.hyperledger.network.server.ServerActor._
import org.hyperledger.network.{HyperLedgerExtension, Version, _}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scalaz.\/
import scalaz.syntax.either._

object ConnectionManager {
  def props = Props(classOf[ConnectionManager])

  case object MaintainConnections

  sealed trait ConnectionEvent {
    def address: InetSocketAddress
  }
  case class PeerUnavailable(address: InetSocketAddress, cause: Throwable) extends ConnectionEvent
  case class ConnectionFailure(address: InetSocketAddress, cause: Throwable) extends ConnectionEvent
  case class Disconnected(address: InetSocketAddress) extends ConnectionEvent
  case class ShookHands(address: InetSocketAddress, version: Version) extends ConnectionEvent
}

class ConnectionManager extends Actor with ActorLogging {

  import ConnectionManager._

  val clock = Clock.systemUTC()

  val hyperLedger = HyperLedgerExtension(context.system)

  val numberOfConnections = hyperLedger.settings.outgoingConnections

  var pendingConnections = Set.empty[InetSocketAddress]
  var connections = ConnectionMaintenance(hyperLedger.NONCE, hyperLedger.settings.discoverySettings.collect {
    case FixedAddressDiscovery(addresses) => addresses
  }.flatten.toSet)

  override def preStart() = {
    import context.dispatcher
    import scala.concurrent.duration._

    bindServer()
    context.system.scheduler.schedule(10 seconds, 1 minute, context.self, MaintainConnections)
  }

  def receive = {
    case MaintainConnections => maintainConnections()
    case PeerUnavailable(address, e) =>
      pendingConnections -= address
      updateConnections(_.unavailable(address, clock.millis()).right)

    case ConnectionFailure(address, cause) =>
      pendingConnections -= address
      updateConnections(_.protocolError(address, cause))

    case Disconnected(address) =>
      pendingConnections -= address
      updateConnections(_.disconnected(address).right)

    case ShookHands(address, version) =>
      updateConnections(_.handshakeComplete(version, address, clock.millis()))

    case scm: ServerBroadcast => context.children.filterNot(_ == sender()).foreach(_ ! scm)
  }

  def updateConnections(f: ConnectionMaintenance => String \/ ConnectionMaintenance) = {
    f(connections).fold({
      error =>
        log.info(s"Closing connection : $error")
        context stop sender()
    }, { newConnections =>
      connections = newConnections
      log.debug(s"Connection count: ${newConnections.numberOfActivePeers}")
    })
  }

  def maintainConnections() = {
    if ((pendingConnections.size + connections.numberOfActivePeers) < numberOfConnections) {
      log.debug(s"Trying to get new connections. Needed: $numberOfConnections Active: ${connections.numberOfActivePeers} Pending: ${pendingConnections.size}")
      connections.addressesForConnection(1, clock.millis(), pi => !pendingConnections(pi.address)).foreach { peer =>
        connectToPeer(peer.address)
        pendingConnections += peer.address
      }
    }
  }
  val codec = Messages.messageCodec(hyperLedger.settings.chain.p2pMagic)
  def connectToPeer(address: InetSocketAddress) = {
    import context.dispatcher
    implicit val mat = ActorMaterializer()

    val peerActor = context.actorOf(PeerConnection.props(address, inbound = false))
    val peerInterface = new DefaultPeerInterface(false, versionToSend, peerActor, hyperLedger.api)

    val (connection, broadcast) = Tcp(context.system)
      .outgoingConnection(address)
      .joinMat(Bitcoin.createProcessingGraph(peerInterface, hyperLedger.api, hyperLedger.ibd, codec))(Keep.both)
      .run

    connection.map(c => PeerConnection.Connected(broadcast)).pipeTo(peerActor)
  }

  private def versionToSend = {
    val ourAddress = hyperLedger.settings.bindAddress.getOrElse(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    hyperLedger.ourVersion(ourAddress)
  }

  private def bindServer() = {
    hyperLedger.settings.bindAddress.foreach { address =>
      import context.dispatcher
      implicit val mat = ActorMaterializer()

      val connections = Tcp(context.system).bind(
        interface = address.getHostString,
        port = address.getPort,
        options = List(Inet.SO.ReuseAddress(true))
      )

      connections.runForeach { connection =>
        val peerActor = context.actorOf(PeerConnection.props(connection.remoteAddress, inbound = true))
        val peerInterface = new DefaultPeerInterface(true, versionToSend, peerActor, hyperLedger.api)

        val graph = Bitcoin.createProcessingGraph(peerInterface, hyperLedger.api, hyperLedger.ibd, codec)
        val broadcast = connection.handleWith(Flow.fromGraph(graph))
        // TODO add broadcast sink to the flow
        peerActor ! PeerConnection.Connected(broadcast)
      }
    }
  }

}

object PeerConnection {
  def props(address: InetSocketAddress, inbound: Boolean) = Props(classOf[PeerConnection], address, inbound)

  case class SendBroadcast(message: BlockchainMessage)
  case class Connected(actor: ActorRef)

  sealed trait PeerState
  case object Handshake extends PeerState
  case object PeerRunning extends PeerState

  sealed trait Data
  case class HandshakeData(broadcast: Option[ActorRef] = None, version: Option[Version] = None) extends Data
  case class PeerData(broadcast: ActorRef, version: Version) extends Data

}

class PeerConnection(address: InetSocketAddress, inbound: Boolean) extends LoggingFSM[PeerConnection.PeerState, PeerConnection.Data] {

  import PeerConnection._
  import ConnectionManager._

  val hyperLedger = HyperLedgerExtension(context.system)

  override def preStart() = {
    log.debug(s"Initializing connection to $address")
  }

  startWith(Handshake, HandshakeData())

  def maybeRunning(hs: HandshakeData) = hs match {
    case HandshakeData(Some(broadcast), Some(version)) =>
      context.parent ! ShookHands(address, version)
      goto(PeerRunning) using PeerData(broadcast, version)
    case _ => stay using hs
  }

  when(Handshake) {
    case Event(c: Connected, hs: HandshakeData) =>
      context.watch(c.actor)
      maybeRunning(hs.copy(broadcast = Some(c.actor)))

    case Event(akka.actor.Status.Failure(error), _) =>
      log.info(s"Unable to connect to peer $address")
      context.parent ! PeerUnavailable(address, error)
      stop()

    case Event(version: Version, hs: HandshakeData) =>
      // TODO we should: (parent ? version).map(ConnectionResponse).pipeTo(self)
      // then:  case Event(RejectionResponse(rejection), _) => stop()
      //        case Event(RejectionResponse(Success(version)), hs) => maybeRunning(hs.copy(version = Some(version))
      if (hyperLedger.versionPredicate(version)) {
        val reply = if (inbound)
          List(VerackMessage())
        else
          List(VerackMessage(), hyperLedger.ibd.createGetHeadersRequest())

        sender() ! \/.right(reply)
        maybeRunning(hs.copy(version = Some(version)))
      } else {
        sender() ! \/.left(RejectMessage(Rejection("version", REJECT_OBSOLETE, "Invalid version")))
        stop() // TODO this might terminates the whole flow before we can send out the rejection?
      }
  }

  when(PeerRunning) {
    case Event(SendBroadcast(msg), PeerData(broadcast, version)) =>
      context.parent ! ServerBroadcast(msg, Some(version.nonce))
      stay()
  }

  whenUnhandled {
    case Event(Terminated(broadcaster), _) =>
      log.debug(s"Broadcaster actor terminated, stopping PeerConnection actor $broadcaster")
      stop()

    case Event(akka.actor.Status.Failure(error), _) =>
      context.parent ! ConnectionFailure(address, error)
      log.error(error, "Peer connection closed with error")
      stop()

    case Event(ServerBroadcast(msg, peerIdOpt), PeerData(broadcast, v)) =>
      if (peerIdOpt.map(_ == v.nonce).getOrElse(true)) {
        broadcast ! msg
      }
      stay()
  }

  onTermination {
    case se => context.parent ! Disconnected(address)
  }

  initialize()
}

object PeerInterface {
  sealed trait Misbehavior
  case class UnexpectedMessage(m: Any) extends Misbehavior
}

trait PeerInterface[M] {
  def inbound: Boolean
  def ourVersion: M
  def versionReceived(version: Version): Future[List[M] \/ List[M]]
  def misbehavior(m: PeerInterface.Misbehavior): Unit
  def broadcast(m: M): Unit
  def latencyReport(latestLatency: FiniteDuration): Unit
}

object DefaultPeerInterface {
  case class HeadersReceived(headers: List[Header])
  case class TransactionReceived(txs: Transaction)
  case class InvReceived(inv: List[InventoryVector])

  val LOG = LoggerFactory.getLogger(classOf[DefaultPeerInterface])
}

class DefaultPeerInterface(val inbound: Boolean, _ourVersion: Version, peerActor: ActorRef, ledger: HyperLedger) extends PeerInterface[BlockchainMessage] {
  import DefaultPeerInterface._
  import PeerInterface._
  import PeerConnection._

  import scala.concurrent.duration._

  implicit val timeout: Timeout = 1 seconds
  val ourVersion = VersionMessage(_ourVersion)

  def misbehavior(m: Misbehavior): Unit = LOG.warn(s"Unexpected message $m".take(160))
  def versionReceived(version: Version) = (peerActor ? version).mapTo[List[BlockchainMessage] \/ List[BlockchainMessage]]
  def broadcast(m: BlockchainMessage) = peerActor ! SendBroadcast(m)
  def latencyReport(latestLatency: FiniteDuration) = println(s"Latency: $latestLatency")
}
