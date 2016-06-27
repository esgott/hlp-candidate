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
package org.hyperledger.pbft.streams

import akka.stream.stage._
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import org.hyperledger.network.{RejectCode, Rejection, Version}
import org.hyperledger.pbft._
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scalaz._

object HandshakeStage {
  type VersionP = Version => String \/ Unit

  def inbound(ourVersion: Version, versionP: VersionP) = new HandshakeStage(ourVersion, versionP, false)
  def outbound(ourVersion: Version, versionP: VersionP) = new HandshakeStage(ourVersion, versionP, true)

  def versionReject(e: String) =
    RejectMessage(Rejection("version", RejectCode.REJECT_OBSOLETE, e))

  val LOG = LoggerFactory.getLogger(classOf[HandshakeStage])
}

class HandshakeStage(ourVersion: Version, versionP: HandshakeStage.VersionP, outbound: Boolean)
  extends GraphStageWithMaterializedValue[BidiShape[PbftMessage, PbftMessage, PbftMessage, PbftMessage], Future[Version]] {
  import HandshakeStage._

  val protoIn = Inlet[PbftMessage]("proto.in")
  val protoOut = Outlet[PbftMessage]("proto.out")
  val appIn = Inlet[PbftMessage]("app.in")
  val appOut = Outlet[PbftMessage]("app.out")

  override def shape: BidiShape[PbftMessage, PbftMessage, PbftMessage, PbftMessage] = new BidiShape(protoIn, appOut, appIn, protoOut)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Version]) = {
    val versionPromise = Promise[Version]

    val logic = new GraphStageLogic(shape) {
      protected var theirVersion: Option[Version] = None
      protected var ack = false

      override def preStart(): Unit = {
        if (outbound) {
          LOG.debug("Sending VersionMessage")
          emit(protoOut, VersionMessage(ourVersion), startHandshake)
        }
        else {
          LOG.debug("Waiting for VersionMessage")
          startHandshake()
        }
      }

      override def postStop(): Unit = {
        versionPromise.tryFailure(new NoSuchElementException("Connection closed before handshake"))
      }

      def startHandshake = () => {
        setHandler(protoIn, new InHandler {
          override def onPush(): Unit = grab(protoIn) match {
            case VersionMessage(version) =>
              versionP(version) match {
                case -\/(reason) =>
                  LOG.debug(s"VersionMessage is rejected: $reason , $version")
                  val ex = InvalidVersionException
                  versionPromise.failure(ex)
                  emit(protoOut, versionReject(reason), () => failStage(ex))
                case \/-(_) =>
                  LOG.debug(s"VersionMessage is accepted: $version")
                  theirVersion = Some(version)
                  val send = if (outbound) List(VerackMessage()) else List(VersionMessage(ourVersion), VerackMessage())
                  emitMultiple(protoOut, send, passAlongAck)
              }
            case VerackMessage(_) =>
              LOG.debug("Received VerackMessage")
              ack = true
              passAlongAck()
            case m =>
              // TODO handle misbehaving peer
              println(s"Misbehaving peer: $m".take(160))
          }
          override def onUpstreamFinish(): Unit = complete(appOut)
        })

        if (!hasBeenPulled(protoIn)) tryPull(protoIn)
      }

      def bidiPassAlong[Out, In <: Out](from: Inlet[In], to: Outlet[Out], msg: String) = {
        class PassAlongHandler extends InHandler with (() => Unit) {
          def apply() = tryPull(from)
          override def onPush(): Unit = {
            val elem = grab(from)
            theirVersion.foreach(v => LOG.debug(s"$msg ${v.addrFrom}: $elem"))
            emit(to, elem, this)
          }
          override def onUpstreamFinish(): Unit = complete(to)
        }

        val ph = new PassAlongHandler
        if (isAvailable(from)) emit(to, grab(from), ph)
        setHandler(from, ph)
        tryPull(from)
      }

      def passAlongAck = () => if (theirVersion.isDefined && ack) {
        LOG.debug("Handshake is complete. Switching to normal flow")
        theirVersion.foreach(versionPromise.success)

        bidiPassAlong(appIn, protoOut, "sending to")
        bidiPassAlong(protoIn, appOut, "receiving from")
      } else {
        pull(protoIn)
      }

      setHandler(protoIn, totallyIgnorantInput)
      setHandler(appOut, new OutHandler {
        override def onPull(): Unit = if (!hasBeenPulled(protoIn)) tryPull(protoIn)
        override def onDownstreamFinish(): Unit = ()
      })

      setHandler(appIn, new InHandler {
        override def onPush(): Unit = ()
        override def onUpstreamFinish(): Unit = complete(protoOut)
      })

      setHandler(protoOut, ignoreTerminateOutput)
    }

    (logic, versionPromise.future)
  }
}

