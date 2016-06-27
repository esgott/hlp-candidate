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

import java.net.{InetAddress, InetSocketAddress}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import org.hyperledger.network.{NetworkAddress, Version}
import org.hyperledger.pbft._
import org.scalatest._

import scala.concurrent.{Await, Future}
import scala.util.Failure
import scalaz.{-\/, \/-}

class HandshakeStageTest extends FunSpec with Matchers {
  import scala.concurrent.duration._
  import HandshakeStageTest._


  implicit val system = ActorSystem("HandshakeTest")
  implicit val mat = ActorMaterializer()

  val ourVersion = createVersion(4)
  val theirVersion = createVersion(0)
  val obsoleteVersion = createVersion(100)

  type AppFlow = Flow[PbftMessage, PbftMessage, NotUsed]

  def defaultAppFlow: AppFlow = Flow[PbftMessage]
  def producingAppFlow(elems: PbftMessage *): AppFlow = {
    Flow.fromSinkAndSource(Sink.ignore, Source.apply[PbftMessage](elems.toList))
  }

  def testflow(stage: HandshakeStage, appFlow: AppFlow, input: PbftMessage*): (Future[Version], Future[Seq[PbftMessage]]) = {
    Source(input.toList)
      .viaMat(BidiFlow.fromGraph(stage).join(appFlow))(Keep.right)
      //.toMat(Sink.fold(Seq.empty[PbftMessage])(_ :+ _))(Keep.both).run
      .toMat(Sink.seq[PbftMessage])(Keep.both).run
  }

  def testVersionP(v: Version) = if (v.nonce == 0) \/-(()) else -\/("test")

  describe("HandshakeStage") {
    describe("for outbound connection") {
      it("should pass through messages after handshake complete") {
        val (version, res) = testflow(HandshakeStage.outbound(ourVersion, testVersionP), defaultAppFlow,
          VersionMessage(theirVersion),
          VerackMessage(),
          PingMessage(Ping(1)),
          PingMessage(Ping(2)))

        val response = Await.result(res, 111.seconds)
        response shouldBe Seq(VersionMessage(ourVersion), VerackMessage(), PingMessage(Ping(1)), PingMessage(Ping(2)))

        Await.result(version, 1 seconds) shouldBe theirVersion
      }
      it("should terminate if version is rejected") {
        val (version, res) = testflow(HandshakeStage.outbound(ourVersion, testVersionP),
          defaultAppFlow,
          VersionMessage(obsoleteVersion),
          VerackMessage())

        Await.ready(version, 1 second)
        version.value shouldBe Some(Failure(InvalidVersionException))

        // TODO The flow failure makes the Future to fail here. Use an ActorRefSink to receive the sent out messages
//        val response = Await.result(res, 1.seconds)
//        response shouldBe Seq(ourVersion, RejectMessage(P.RejectMessage("tx", P.REJECT_OBSOLETE, "test", None)))
      }
      it ("should send out message right after handshake complete") {
        val (version, res) = testflow(HandshakeStage.outbound(ourVersion, testVersionP),
          producingAppFlow(PingMessage(Ping(12))),
          VersionMessage(theirVersion),
          VerackMessage())

        val response = Await.result(res, 1.seconds)
        response shouldBe Seq(VersionMessage(ourVersion), VerackMessage(), PingMessage(Ping(12)))

        Await.result(version, 1 seconds) shouldBe theirVersion
      }
    }
    describe("for inbound connection") {
      it ("should pass through messages after handshake complete") {
        val (version, res) = testflow(HandshakeStage.inbound(ourVersion, testVersionP), defaultAppFlow,
          VersionMessage(theirVersion),
          VerackMessage(),
          PingMessage(Ping(1)),
          PingMessage(Ping(2)))
        val response = Await.result(res, 1.seconds)
        response shouldBe Seq(VersionMessage(ourVersion), VerackMessage(), PingMessage(Ping(1)), PingMessage(Ping(2)))

        Await.result(version, 1 seconds) shouldBe theirVersion
      }
      it ("should send out message right after handshake complete") {
        val (version, res) = testflow(HandshakeStage.inbound(ourVersion, testVersionP),
          producingAppFlow(PingMessage(Ping(12))),
          VersionMessage(theirVersion),
          VerackMessage())
        val response = Await.result(res, 1.seconds)
        response shouldBe Seq(VersionMessage(ourVersion), VerackMessage(), PingMessage(Ping(12)))

        Await.result(version, 1 seconds) shouldBe theirVersion
      }
    }
  }

}

object HandshakeStageTest {
  def createVersion(nonce: Long) = Version.forNow(
    0L,
    Version.SIMPLE_NODE,
    NetworkAddress.forVersion(Version.SIMPLE_NODE, new InetSocketAddress(InetAddress.getLocalHost, 8555)),
    NetworkAddress.forVersion(Version.SIMPLE_NODE, new InetSocketAddress(InetAddress.getLocalHost, 8666)),
    nonce,
    "Test Node",
    3, relay = true)
}
