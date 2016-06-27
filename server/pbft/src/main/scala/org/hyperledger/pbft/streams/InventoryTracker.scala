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

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import org.hyperledger.network._
import org.hyperledger.pbft._

import scala.collection.immutable.Seq
import scala.collection.mutable

object InventoryTracker {

  case class Shape(peerInput: Inlet[InvMessage], broadcastInput: Inlet[InvMessage], out: Outlet[InvMessage]) extends akka.stream.Shape {
    override def inlets = List(peerInput, broadcastInput)
    override def outlets = List(out)
    override def deepCopy(): Shape = new Shape(peerInput.carbonCopy(), broadcastInput.carbonCopy(), out.carbonCopy())
    override def copyFromPorts(inlets: Seq[Inlet[_]], outlets: Seq[Outlet[_]]): Shape = {
      require(inlets.size == 2, s"proposed inlets [${inlets.mkString(", ")}] do not fit InventoryTracker.Shape")
      require(outlets.size == 1, s"proposed outlets [${outlets.mkString(", ")}] do not fit InventoryTracker.Shape")
      new Shape(inlets.head.as[InvMessage],
        inlets.tail.head.as[InvMessage],
        outlets.head.as[InvMessage])
    }
  }

  def apply(cacherSize: Int) = new InventoryTracker(cacherSize)
}

class InventoryTracker(cacheSize: Int) extends GraphStage[InventoryTracker.Shape] {
  val peerIn = Inlet[InvMessage]("peerIn")
  val broadcastIn = Inlet[InvMessage]("broadcastIn")
  val out = Outlet[InvMessage]("out")

  override val shape = new InventoryTracker.Shape(peerIn, broadcastIn, out)
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    val cache = mutable.LinkedHashSet.empty[InventoryVector]

    override def preStart() = {
      pull(peerIn)
      pull(broadcastIn)
    }

    setHandler(peerIn, new InHandler {
      override def onPush(): Unit = {
        val elem = grab(peerIn)
        cache ++= elem.payload
        if (cache.size > cacheSize) {
          cache.take(cache.size - cacheSize).foreach(cache.remove)
        }
        tryPull(peerIn)
      }
    })

    setHandler(broadcastIn, new InHandler {
      override def onPush(): Unit = {
        val elem = grab(broadcastIn)
        val unknownInv = elem.payload.filterNot(cache)
        cache ++= elem.payload
        if (unknownInv.nonEmpty) {
          emit(out, InvMessage(unknownInv))
        } else {
          tryPull(broadcastIn)
        }
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (!hasBeenPulled(broadcastIn)) tryPull(broadcastIn)
      }
    })

  }
}
