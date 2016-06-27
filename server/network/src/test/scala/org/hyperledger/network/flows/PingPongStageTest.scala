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

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.QueueOfferResult.Enqueued
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.testkit.{ImplicitSender, TestKit}
import org.hyperledger.network.Messages.{PingMessage, PongMessage, VerackMessage}
import org.hyperledger.network.Ping
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}

import scala.concurrent.{Await, Channel, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.language.postfixOps

class PingPongStageTest extends TestKit(ActorSystem("InitialBlockDownloadStageTest"))
  with FunSpecLike with ImplicitSender with BeforeAndAfter with Matchers {

  import PingPongStage._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val mat = ActorMaterializer()

  var reportedPingTimes = List.empty[FiniteDuration]
  val monitorChannel = new Channel[FiniteDuration]
  def latencyMonitor(dur: FiniteDuration): Unit = {
    reportedPingTimes :+= dur
    monitorChannel.write(dur)
  }

  val stage = PingPongStage(latencyMonitor, PingPongSettings(1 seconds, 3 seconds))

  def testflow = Source.queue(100, OverflowStrategy.fail)
    .viaMat(stage)(Keep.left)
    .toMat(Sink.queue())(Keep.both)
    .run

  before {
    reportedPingTimes = List.empty[FiniteDuration]
  }

  describe("PingPongStage") {
    it("should send out a ping immediately") {
      val (in, out) = testflow
      val Some(PingMessage(Ping(nonce1))) = Await.result(out.pull(), 100 millis)
      val consumed1 = Await.result(in.offer(PongMessage(Ping(nonce1))), 100 millis)
      consumed1 shouldBe Enqueued

      monitorChannel.read
      reportedPingTimes.size shouldBe 1
    }
    it("should timeout out and complete when timed out") {
      val (in, out) = testflow
      val Some(PingMessage(Ping(nonce1))) = Await.result(out.pull(), 100 millis)

      Await.result(Future(Thread.sleep(TimeUnit.SECONDS.toMillis(5))), 6 seconds)

      Await.ready(in.offer(PongMessage(Ping(nonce1))), 100 millis) onComplete {
        case Success(res) => fail("PingPongStage should have timed out")
        case Failure(e)   => println(e)
      }
    }

    it("should ignore pong with unknown nonce") {
      val (in, out) = testflow
      val Some(PingMessage(Ping(nonce1))) = Await.result(out.pull(), 100 millis)
      Await.ready(in.offer(PongMessage(Ping(nonce1 + 10))), 100 millis)

      reportedPingTimes shouldBe 'empty

      Await.ready(in.offer(PongMessage(Ping(nonce1))), 100 millis)

      monitorChannel.read
      reportedPingTimes.size shouldBe 1
    }
    it("should response with Pong to Pings") {
      val (in, out) = testflow
      val ping = PingMessage(Ping(123))
      Await.ready(in.offer(ping), 100 millis)
      val Some(PingMessage(Ping(_))) = Await.result(out.pull(), 100 millis)
      val Some(PongMessage(Ping(nonce))) = Await.result(out.pull(), 100 millis)
      nonce shouldBe 123
    }
  }
}
