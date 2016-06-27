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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{TestKit, TestProbe}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.hyperledger.pbft._
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Await

class PbftMessageTest extends TestKit(ActorSystem("test", ConfigFactory.load(ConfigFactory.parseString(PbftLogicTest.config))))
  with WordSpecLike with Matchers {

  import TestBlockStore._
  import system.dispatcher

  import scala.concurrent.duration._

  implicit val mat = ActorMaterializer()

  val extension = PbftExtension(system)
  val store = new TestBlockStore
  val settings: PbftSettings = PbftSettings.fromConfig(ConfigFactory.parseString(
      """
        |hyperledger {
        |  pbft {
        |    bindAddress: "127.0.0.1:8551"
        |    privateKey: "L2uwgqBffvx4KXjbdiVQdFfx4ALrrCbdr2ru7ucjPtbWjPnFCU8e"
        |    nodes: [
        |      {address: "127.0.0.1:8551", publicKey: "031a8027f5c2ab3f4e3e76c8c38f754484241bb1002b44f8b83217c47bb7cc0a87"},
        |      {address: "127.0.0.1:8552", publicKey: "0316f8caac24fb7dc113f21c870166700bc33377370ed9b3c3b2566858e279522b"}
        |      {address: "127.0.0.1:8553", publicKey: "0298751746d1456c8235922a6656caae5c7e7a1ca581de55248b3c3d24ad8daf63"}
        |    ]
        |    protocolTimeoutSeconds: 60
        |    blockFrequencySeconds: 10
        |  }
        |}
      """.stripMargin))

  extension.blockStoreConn = store

  val broadcaster = TestProbe()
  val miner = TestProbe()
  val broadcastOut = TestProbe()
  val handler = system.actorOf(PbftHandler.props(broadcaster.ref, miner.ref))
  val address = new InetSocketAddress(InetAddress.getLocalHost, 8551)

  val ourVersion = HandshakeStageTest.createVersion(4)
  val theirVersion = HandshakeStageTest.createVersion(0)

  val flow = PbftStreams.createFlow(settings, store, extension.versionP, broadcastOut.ref, handler, ourVersion, outbound = true)
  val codec = PbftMessage.messageCodec(PbftStreams.NETWORK_MAGIC)

  "Sending in a PrePrepare to the flow" should {
    "trigger Prepare on the broadcaster" in {
      val prePrepareMessage =  PrePrepareMessage(PrePrepare(0, 0, dummyBlock).sign(settings.privateKey.get).require)
      val input: List[PbftMessage] = List(VersionMessage(theirVersion), VerackMessage(), prePrepareMessage)
      val inputBytes: List[ByteString] = input.map { m => ByteString(codec.encode(m).require.toByteBuffer) }

      val future = Source(inputBytes).via(flow).runWith(Sink.fold(Seq.empty[ByteString])(_ :+ _))
      Await.result(future, 10 second)

      val message = broadcaster.receiveOne(1 second).asInstanceOf[PrepareMessage]
      message shouldNot be(null)
    }
  }
}
