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
package org.hyperledger.pbft

import java.net.InetSocketAddress

import akka.actor._
import akka.pattern._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp
import org.hyperledger.network.Version
import org.hyperledger.pbft.streams.PbftStreams

import scalaz.{ -\/, \/- }

object PbftServer {
  case class ConnectionPending(address: InetSocketAddress)
  case class ConnectionFailed(address: InetSocketAddress, e: Throwable)

  sealed trait PbftConnectionEvent
  case class NewConnection(
    broadcast: ActorRef,
    connection: InetSocketAddress) extends PbftConnectionEvent
  case class HandshakeComplete(
    connectionAddress: InetSocketAddress,
    version: Version,
    peer: ActorRef) extends PbftConnectionEvent
  case class Broadcast(message: PbftMessage) extends PbftConnectionEvent

  def keep3[L, LI, RI](l: L, r: (LI, RI)) = (l, r._1, r._2)

  def props(bindAddress: InetSocketAddress) = Props(new PbftServer(bindAddress))
}

class PbftServer(bindAddress: InetSocketAddress) extends Actor with ActorLogging {
  import PbftServer._

  val pbft = PbftExtension(context.system)
  val settings = pbft.settings
  var connections = ConnectionManagement.empty
  var recoveryStarted = false

  val miner = context.actorOf(PbftMiner.props())

  val handlerProps = if (settings.clientMode) PbftClient.props() else PbftHandler.props(context.self, miner)
  val handler = context.actorOf(handlerProps)

  override def preStart = {
    import context.dispatcher
    implicit val mat = ActorMaterializer()

    log.debug("Starting PBFT Server")

    val self = context.self
    val address = settings.bindAddress
    val ourVersion = pbft.ourVersion(address)

    for (nodeConfig <- settings.otherNodes) {
      log.debug(s"Trying to connect to $nodeConfig")
      val (conn, version, broadcaster) = Tcp(context.system)
        .outgoingConnection(nodeConfig.address)
        .joinMat(PbftStreams.createFlow(settings, pbft.blockStoreConn, pbft.versionP, context.self, handler, ourVersion, outbound = true))(keep3)
        .run()

      conn.map(q => NewConnection(broadcaster, q.remoteAddress))
        .recover { case e: Throwable => ConnectionFailed(nodeConfig.address, e) }
        .pipeTo(self)

      version.map(v => HandshakeComplete(nodeConfig.address, v, broadcaster)).pipeTo(self)
    }

    Tcp(context.system).bind(address.getHostString, address.getPort).runForeach { connection =>
      val flow = PbftStreams.createFlow(settings, pbft.blockStoreConn, pbft.versionP, context.self, handler, ourVersion, outbound = false)
      val (versionFuture, broadcaster) = connection.handleWith(flow)

      versionFuture.map(v => HandshakeComplete(connection.remoteAddress, v, broadcaster)).pipeTo(self)
      self ! NewConnection(broadcaster, connection.remoteAddress)
    }
  }

  def receive = {
    case Terminated(a) =>
      log.debug(s"child terminated $a")

    case NewConnection(broadcaster, remoteAddress) =>
      log.debug(s"New connection received from $remoteAddress")
      context.watch(broadcaster)

      connections.connected(broadcaster, remoteAddress) match {
        case \/-(newConnections) => connections = newConnections
        case -\/(e)              => broadcaster ! PoisonPill
      }

    case ConnectionFailed(address, error) => connections = connections.timeout(address)

    case HandshakeComplete(connectionAddress, version, peer) =>
      val (newConnections, _) = connections.handshakeComplete(settings, peer, connectionAddress, version)
      connections = newConnections
      if (!recoveryStarted) {
        peer ! pbft.getHeadersMessage
        recoveryStarted = true
      }

    case m: PbftMessage => connections.activeConnections.values.foreach(_.peer ! m)
  }
}
