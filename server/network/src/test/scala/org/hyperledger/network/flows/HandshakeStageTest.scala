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
package org.hyperledger.network.flows

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.testkit.TestKit
import org.hyperledger.network.Messages._
import org.hyperledger.network.RejectCode.REJECT_OBSOLETE
import org.hyperledger.network._
import org.hyperledger.network.flows.HandshakeStage._
import org.hyperledger.network.server.PeerInterface
import org.hyperledger.network.server.PeerInterface.{Misbehavior, UnexpectedMessage}
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.{Await, Future}
import scalaz.\/

class HandshakeStageTest extends TestKit(ActorSystem("HandshakeStageTest")) with FunSpecLike with Matchers {
  import HandshakeStageTest._
  import TestUtils._

  import scala.concurrent.duration._

  implicit val mat = ActorMaterializer()
  implicit val ec = scala.concurrent.ExecutionContext.global

  val _ourVersion = createVersion(4)
  val theirVersion = createVersion(0)
  val obsoleteVersion = createVersion(100)

  class TestPeerInterface(val inbound: Boolean) extends PeerInterface[BlockchainMessage] {
    @volatile var invalidMsg = List.empty[Misbehavior]

    val ourVersion = VersionMessage(_ourVersion)
    override def versionReceived(version: Version): Future[List[BlockchainMessage] \/ List[BlockchainMessage]] =
      if (version.nonce == 100)
        Future.successful(\/.left(List(RejectMessage(Rejection("version", REJECT_OBSOLETE, "", None)))))
      else
        Future.successful(\/.right(List(VerackMessage())))

    override def misbehavior(m: Misbehavior): Unit = invalidMsg = invalidMsg :+ m

    def assertMisbehavior(f: List[Misbehavior] => Unit) = {
      f(invalidMsg)
      invalidMsg = List.empty[Misbehavior]
    }

    def broadcast(m: BlockchainMessage) = ()
    def latencyReport(latestLatency: FiniteDuration) = println(s"Latency: $latestLatency")
  }

  val handler: HandshakeHandler[BlockchainMessage] = {
    case VersionMessage(v) => VersionReceived(v)
    case VerackMessage() => VersionAcked
    case RejectMessage(r@Rejection("version", code, reason, data)) => VersionRejected(r)
    case m => NonHandshakeMessage(m)
  }

  def stageUnderTest(peer: TestPeerInterface) = new HandshakeStage(peer, handler)
  def testflow(appFlow: Flow[BlockchainMessage, BlockchainMessage, NotUsed], stage: HandshakeStage[BlockchainMessage], input: BlockchainMessage*) = {
    Source(input.toList)
      .via(BidiFlow.fromGraph(stage).join(appFlow))
      .toMat(Sink.seq[BlockchainMessage])(Keep.right)
      .run
  }

  describe("HandshakeStage") {
    describe("for outbound connection") {
      val peer = new TestPeerInterface(false)
      val stage = stageUnderTest(peer)

      it("should pass through messages after handshake complate") {
        val result = testflow(defaultAppFlow, stage, VersionMessage(theirVersion), VerackMessage(), PingMessage(Ping(1)), PingMessage(Ping(2)))
        val response = Await.result(result, 1 seconds)

        response shouldBe Seq(VersionMessage(_ourVersion), VerackMessage(), PingMessage(Ping(1)), PingMessage(Ping(2)))
      }
      it("should terminate if version is rejected") {
        val result = testflow(defaultAppFlow, stage, VersionMessage(obsoleteVersion), VerackMessage())
        val response = Await.result(result, 1 second)

        response shouldBe Seq(VersionMessage(_ourVersion), RejectMessage(Rejection("version", REJECT_OBSOLETE, "", None)))
      }
      it("should send out message right after handshake is complete") {
        val result = testflow(producingAppFlow(PingMessage(Ping(12))), stage, VersionMessage(theirVersion), VerackMessage())
        val response = Await.result(result, 1 second)

        response shouldBe Seq(VersionMessage(_ourVersion), VerackMessage(), PingMessage(Ping(12)))
      }
      it("should report of rejection") {
        val result = testflow(defaultAppFlow, stage, VersionMessage(theirVersion), RejectMessage(Rejection("version", REJECT_OBSOLETE, "", None)))
        val RejectionException(_, rejection) = intercept[RejectionException] {
          Await.result(result, 1 second)
        }
        rejection.ccode shouldBe REJECT_OBSOLETE
        rejection.message shouldBe "version"
      }
      it("should report misbehavior") {
        val result = testflow(defaultAppFlow, stage, VersionMessage(theirVersion), GetAddrMessage(), VerackMessage())
        val response = Await.result(result, 1 second)
        response shouldBe Seq(VersionMessage(_ourVersion), VerackMessage())
        peer.assertMisbehavior(_ shouldBe List(UnexpectedMessage(GetAddrMessage())))
      }
    }
    describe("for inbound connection") {
      val peer = new TestPeerInterface(true)
      val stage = stageUnderTest(peer)
      it ("should pass through message after handshake complete") {
        val result = testflow(defaultAppFlow, stage, VersionMessage(theirVersion), VerackMessage(), PingMessage(Ping(1)), PingMessage(Ping(2)))
        val response = Await.result(result, 100 second)

        response shouldBe Seq(VersionMessage(_ourVersion), VerackMessage(), PingMessage(Ping(1)), PingMessage(Ping(2)))
      }
      it("should send out messages right after the handshake is complete") {
        val result = testflow(producingAppFlow(PingMessage(Ping(12))), stage, VersionMessage(theirVersion), VerackMessage())
        val response = Await.result(result, 1 second)

        response shouldBe Seq(VersionMessage(_ourVersion), VerackMessage(), PingMessage(Ping(12)))
      }
    }
  }

}

object HandshakeStageTest {
//  def createVersion(nonce: Long) = Version.forNow(
//    0L,
//    Version.SIMPLE_NODE,
//    NetworkAddress.forVersion(Version.SIMPLE_NODE, new InetSocketAddress(InetAddress.getLocalHost, 8555)),
//    NetworkAddress.forVersion(Version.SIMPLE_NODE, new InetSocketAddress(InetAddress.getLocalHost, 8666)),
//    nonce,
//    "Test Node",
//    3, relay = true)

  def defaultAppFlow = Flow[BlockchainMessage]
  def producingAppFlow(elems: BlockchainMessage*): Flow[BlockchainMessage, BlockchainMessage, NotUsed] = {
    Flow.fromSinkAndSource(Sink.ignore, Source.apply[BlockchainMessage](elems.toList))
  }
}
