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

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ GraphDSL, Keep, Sink, Source, SourceQueue }
import akka.stream.{ ActorMaterializer, OverflowStrategy, SourceShape }
import org.hyperledger.common.TID
import org.hyperledger.network.InventoryVector
import org.hyperledger.pbft.InvMessage
import org.scalatest.{ FunSpec, Matchers }

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class InventoryTrackerTest extends FunSpec with Matchers {
  val CACHE_SIZE = 3

  implicit val system = ActorSystem("InventoryTrackerTest")
  import system.dispatcher
  implicit val mat = ActorMaterializer()

  val ZERO = Array.fill[Byte](32)(0)
  def txinv(hashBytes: Byte*) = InventoryVector.tx(new TID(hashBytes.toArray ++ ZERO.drop(hashBytes.length)))

  def testflow[T](f: (SourceQueue[InvMessage], SourceQueue[InvMessage]) => Future[T])(assertion: Seq[InvMessage] => Unit) = {
    val input = Source.queue[InvMessage](10, OverflowStrategy.dropTail)
    val broadcastInput = Source.queue[InvMessage](10, OverflowStrategy.dropTail)
    val graph = GraphDSL.create(input, broadcastInput)(Keep.both) { implicit b =>
      (i, bi) =>
        import GraphDSL.Implicits._

        val tracker = b.add(InventoryTracker(CACHE_SIZE))
        i ~> tracker.peerInput
        bi ~> tracker.broadcastInput

        SourceShape.of(tracker.out)
    }

    val ((inputQ, broadcastQ), result) = Source.fromGraph(graph).toMat(Sink.seq[InvMessage])(Keep.both).run()

    val offers = f(inputQ, broadcastQ)
    Await.ready(offers, 10 seconds)
    inputQ.complete()
    assertion(Await.result(result, 10 seconds))
  }

  describe("InventoryTracker stage") {
    describe("when no message received from the peer yet") {
      it("should send first broadcast out") {
        testflow((_, b) => Future.sequence(Seq(
          b.offer(InvMessage(List(txinv(1, 2)))),
          b.offer(InvMessage(List(txinv(3, 4))))))) {
          _ shouldBe Seq(
            InvMessage(List(txinv(1, 2))),
            InvMessage(List(txinv(3, 4))))
        }
      }
      it("should not send the same broadcast twice") {
        testflow((_, b) => Future.sequence(Seq(
          b.offer(InvMessage(List(txinv(1, 2)))),
          b.offer(InvMessage(List(txinv(3, 4)))),
          b.offer(InvMessage(List(txinv(1, 2))))))) {
          _ shouldBe Seq(
            InvMessage(List(txinv(1, 2))),
            InvMessage(List(txinv(3, 4))))
        }
      }
    }
    describe("when some inv messages already received") {
      it("should not send the same broadcast") {
        testflow((i, b) => Future.sequence(Seq(
          i.offer(InvMessage(List(txinv(1, 2)))),
          b.offer(InvMessage(List(txinv(1, 2)))),
          b.offer(InvMessage(List(txinv(3, 4))))))) {
          _ shouldBe Seq(
            InvMessage(List(txinv(3, 4))))
        }
      }
    }

    describe("when the received inv messages fills the cache") {
      it("should send broadcast for invs removed from the cache") {
        testflow((i, b) => Future.sequence(Seq(
          i.offer(InvMessage(List(txinv(1, 2)))),
          i.offer(InvMessage(List(txinv(3, 4)))),
          i.offer(InvMessage(List(txinv(5, 6)))),
          i.offer(InvMessage(List(txinv(7, 8)))),
          i.offer(InvMessage(List(txinv(9, 0)))),
          b.offer(InvMessage(List(txinv(5, 6)))),
          b.offer(InvMessage(List(txinv(3, 4)))),
          b.offer(InvMessage(List(txinv(1, 2))))))) {
          _ shouldBe Seq(
            InvMessage(List(txinv(3, 4))),
            InvMessage(List(txinv(1, 2)))
          )
        }

      }
    }
  }

}
