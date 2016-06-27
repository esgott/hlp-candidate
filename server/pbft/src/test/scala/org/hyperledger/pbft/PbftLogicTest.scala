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

import akka.actor.FSM.{ CurrentState, SubscribeTransitionCallBack, UnsubscribeTransitionCallBack }
import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ TestFSMRef, TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import org.hyperledger.common._
import org.hyperledger.pbft.BlockHandler.ConsensusTimeout
import org.hyperledger.pbft.Consensus.{ WaitForCommit, WaitForPrepare }
import org.hyperledger.pbft.PbftLogicTest._
import org.hyperledger.pbft.RecoveryActor.NewHeaders
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpecLike }
import scodec.Codec
import TestBlockStore._

class PbftLogicTest extends TestKit(ActorSystem("test", ConfigFactory.load(ConfigFactory.parseString(config))))
  with WordSpecLike with BeforeAndAfterEach with Matchers {
  import PbftHandler._

  import scala.concurrent.duration._

  val distributer = TestProbe()
  val miner = TestProbe()
  val extension = PbftExtension(system)
  val store = new TestBlockStore
  extension.blockStoreConn = store
  var pbft: TestFSMRef[PbftHandlerState, PbftHandlerData, PbftHandler] = _

  override def beforeEach(): Unit = {
    store.reset()
    pbft = TestFSMRef(new PbftHandler(distributer.ref, miner.ref))
  }

  override def afterEach(): Unit = {
    pbft.stop()
  }

  def send(m: PrePrepare) = pbft ! PrePrepareMessage(m)
  def send(m: Prepare) = pbft ! PrepareMessage(m)
  def send(m: Commit) = pbft ! CommitMessage(m)
  def send(m: ViewChange) = pbft ! ViewChangeMessage(m)
  def send(m: NewView) = pbft ! NewViewMessage(m)

  def receive[T](check: T => Unit) = {
    val response = distributer.receiveOne(1 seconds).asInstanceOf[T]
    if (response == null) fail("No message received")
    system.log.info("Got message: " + response)
    check(response.asInstanceOf[T])
  }

  def receiveNoMessage() = distributer.expectNoMsg(200 millis)

  def checkPrepare(node: Int, viewSeq: Int, header: Header)
                  (actual: PrepareMessage) = {
    actual.payload.node shouldEqual node
    actual.payload.viewSeq shouldEqual viewSeq
    actual.payload.blockHeader.getID shouldEqual header.getID
  }

  def checkCommit(node: Int, viewSeq: Int, header: Header)
                 (actual: CommitMessage) = {
    actual.payload.node shouldEqual node
    actual.payload.viewSeq shouldEqual viewSeq
    actual.payload.blockHeader.getID shouldEqual header.getID
  }

  def checkViewChange(node: Int, viewSeq: Int, header: Header, commits: List[Commit])
                     (actual: ViewChangeMessage) = {
    actual.payload.node shouldEqual node
    actual.payload.viewSeq shouldEqual viewSeq
    actual.payload.blockHeader.getID shouldEqual header.getID
    actual.payload.commits.size shouldEqual commits.size
    actual.payload.commits.toSet shouldEqual commits.toSet
  }

  def checkNewView(node: Int, viewSeq: Int, viewChanges: List[ViewChange], blockHeader: Header, commits: List[Commit])
                  (actual: NewViewMessage) = {
    actual.payload.node shouldEqual node
    actual.payload.viewSeq shouldEqual viewSeq
    actual.payload.viewChanges.toSet shouldEqual viewChanges.toSet
    actual.payload.blockHeader.getID shouldEqual blockHeader.getID
    actual.payload.commits.size shouldEqual commits.size
    actual.payload.commits.toSet shouldEqual commits.toSet
  }

  def checkGetHeaders(actual: GetHeadersMessage) = {
    actual.payload.version shouldEqual extension.PROTOCOL_VERSION
    actual.payload.hashStop shouldEqual BID.INVALID
  }

  def checkState[StateT](worker: ActorRef, state: StateT) = {
    val transitionListener = TestProbe()
    worker ! SubscribeTransitionCallBack(transitionListener.ref)
    transitionListener.expectMsg(CurrentState(worker, state))
    worker ! UnsubscribeTransitionCallBack(transitionListener.ref)
  }

  def getConsensusChild(id: BID): ActorRef = {
    val name = s"consensus-$id"
    pbft.getSingleChild("blockhandler").getChild(List(name).iterator)
  }

  def checkNoActiveConsensus(id: BID) = {
    val child = getConsensusChild(id)
    child.toString shouldNot contain(id.toString)
  }

  def getDummyConsensus = getConsensusChild(dummyBlock.getID)

  "PrePrepare" should {
    "trigger Prepare" in {
      send(PrePrepare(0, 0, dummyBlock))
      receive(checkPrepare(1, 0, dummyBlock.getHeader))
      checkState(getDummyConsensus, WaitForPrepare)
    }

    "not trigger Prepare from other view" in {
      send(PrePrepare(0, 2, dummyBlock))
      checkNoActiveConsensus(dummyBlock.getID)
    }

    "not trigger Prepare from non-primary node" in {
      send(PrePrepare(2, 2, dummyBlock))
      checkNoActiveConsensus(dummyBlock.getID)
    }
  }

  "Prepare" should {
    "trigger Commit after received from 2f-1 distinct nodes" ignore {
      send(PrePrepare(0, 0, dummyBlock))
      receive(checkPrepare(1, 0, dummyBlockHeader))
      send(Prepare(2, 0, dummyBlockHeader))
      send(Prepare(3, 0, dummyBlockHeader))
      send(Prepare(3, 0, dummyBlockHeader))
      checkState(getDummyConsensus, WaitForPrepare)
      send(Prepare(4, 0, dummyBlockHeader))
      receive(checkCommit(1, 0, dummyBlockHeader))
      checkState(getDummyConsensus, WaitForCommit)
    }

    "not trigger Commit from other view" ignore {
      send(PrePrepare(0, 0, dummyBlock))
      receive(checkPrepare(1, 0, dummyBlockHeader))
      send(Prepare(2, 0, dummyBlockHeader))
      send(Prepare(3, 0, dummyBlockHeader))
      send(Prepare(4, 1, dummyBlockHeader))
      checkState(getDummyConsensus, WaitForPrepare)
    }

    "not trigger Commit when Prepare is for other block" ignore {
      val dummyBID2 = BID.createFromSafeArray(dummyHash(2))
      val dummyBlockHeader2 = new BitcoinHeader(1, dummyBID2, dummyMerkleRoot, 0, 1, 2)

      send(PrePrepare(0, 0, dummyBlock))
      receive(checkPrepare(1, 0, dummyBlockHeader))
      send(Prepare(2, 0, dummyBlockHeader))
      send(Prepare(3, 0, dummyBlockHeader))
      send(Prepare(4, 0, dummyBlockHeader2))
      checkState(getDummyConsensus, WaitForPrepare)
    }
  }

  "Prepares before PrePrepare" should {
    "should also trigger Commit" ignore {
      send(Prepare(2, 0, dummyBlockHeader))
      send(Prepare(3, 0, dummyBlockHeader))
      send(Prepare(4, 0, dummyBlockHeader))
      send(PrePrepare(0, 0, dummyBlock))
      receive(checkPrepare(1, 0, dummyBlockHeader))
      receive(checkCommit(1, 0, dummyBlockHeader))
    }
  }

  "Commit" should {
    "store the block after 2f arrives" ignore {
      send(PrePrepare(0, 0, dummyBlock))
      receive(checkPrepare(1, 0, dummyBlockHeader))
      send(Prepare(2, 0, dummyBlockHeader))
      send(Prepare(3, 0, dummyBlockHeader))
      send(Prepare(4, 0, dummyBlockHeader))
      send(Prepare(6, 0, dummyBlockHeader))
      receive(checkCommit(1, 0, dummyBlockHeader))

      val commits = (2 to 5)
        .map(Commit(_, 0, dummyBlockHeader))
        .toList
      commits.filter { _.node < 5 }.foreach(send)

      store.blockStore shouldEqual store.genesis

      send(commits.last)

      receiveNoMessage()
      val ownCommit = Commit(1, 0, dummyBlockHeader).sign(keys(1)).require
      store.blockStore.size shouldEqual 2
      store.blockStore(1)._1 shouldEqual dummyBlock
      store.blockStore(1)._2.toSet shouldEqual (ownCommit :: commits).toSet
    }

  }

  "Timeout" should {
    "trigger ViewChange for next view" ignore {
      send(Prepare(0, 0, dummyBlockHeader))
      pbft ! ConsensusTimeout()
      receive(checkViewChange(1, 1, genesisBlockHeader, List.empty[Commit]))
    }
  }

  "NewView" should {
    "take PbftHandler to new view with 2f ViewChange messages with valid signature" ignore {
      pbft.stateData.currentViewSeq shouldEqual 0

      val commits = (2 to 6)
        .map(node => Commit(node, 0, dummyBlockHeader).sign(keys(node)).require)
        .toList
      val viewChanges = (2 to 6)
        .map(node => ViewChange(node, 1, dummyBlockHeader, commits).sign(keys(node)).require)
        .toList
      store.store(dummyBlock, commits)

      send(NewView(2, 1, viewChanges, dummyBlockHeader, commits))

      receiveNoMessage()
      pbft.stateData.currentViewSeq shouldEqual 1
    }

    "take PbftHandler to new view after recovery" ignore {
      pbft.stateData.currentViewSeq shouldEqual 0

      val commits = (2 to 6)
        .map(node => Commit(node, 0, dummyBlockHeader2).sign(keys(node)).require)
        .toList
      val viewChanges = (2 to 6)
        .map(node => ViewChange(node, 1, dummyBlockHeader, commits).sign(keys(node)).require)
        .toList
      store.store(dummyBlock, List.empty)

      send(NewView(2, 1, viewChanges, dummyBlockHeader2, commits))
      receive(checkGetHeaders)
      receiveNoMessage()
      store.callBlockListener(dummyBlockHeader2.getID)
      receiveNoMessage()
      pbft.stateData.currentViewSeq shouldEqual 1
    }

    "not take PbftHandler to new view when arrives for a past block" ignore {
      val viewChanges = (2 to 6)
        .map(node => ViewChange(node, 1, dummyBlockHeader2, List.empty).sign(keys(node)).require)
        .toList

      send(NewView(2, 1, viewChanges, dummyBlockHeader2, List.empty))

      receiveNoMessage()
      pbft.stateData.currentViewSeq shouldEqual 0
    }

    "not take PbftHandler to new view when commits has invalid signatures" ignore {
      val viewChanges = (2 to 6)
        .map(node => ViewChange(node, 1, dummyBlockHeader, List.empty).sign(keys(node)).require)
        .toList
      val commits = (2 to 5)
        .map(node => Commit(node, 0, dummyBlockHeader2).sign(keys(node)).require)
        .toList
      val invalidCommit = Commit(6, 0, dummyBlockHeader2)

      send(NewView(2, 1, viewChanges, dummyBlockHeader2, commits :+ invalidCommit))

      receiveNoMessage()
      pbft.stateData.currentViewSeq shouldEqual 0
    }

    "not take PbftHandler to new view without 2f ViewChange in it" ignore {
      send(NewView(2, 1, List.empty, dummyBlockHeader, List.empty))

      receiveNoMessage()
      pbft.stateData.currentViewSeq shouldEqual 0
    }

    "not take PbftHandler to new view without valid signature" ignore {
      val viewChanges = (2 to 5)
        .map(node => ViewChange(node, 1, dummyBlockHeader, List.empty).sign(keys(node)).require)
        .toList
      val invalidViewChange = ViewChange(6, 1, dummyBlockHeader, List.empty)

      send(NewView(2, 1, viewChanges :+ invalidViewChange, dummyBlockHeader, List.empty))

      receiveNoMessage()
      pbft.stateData.currentViewSeq shouldEqual 0
    }
  }

  // Set to ignore as it's flaky
  "ViewChange" ignore {
    "trigger NewView after 2f received and node is primary" ignore {
      val commits = (2 to 6)
        .map(node => Commit(node, 1, dummyBlockHeader).sign(keys(node)).require)
        .toList
      val viewChanges = (2 to 6)
        .map(ViewChange(_, 1, dummyBlockHeader, commits))
        .toList

      viewChanges.filter(_.node < 6).foreach(send)

      // ViewChange after f+1 ViewChanges
      receive(checkViewChange(1, 1, genesisBlockHeader, List.empty))

      send(viewChanges.last)

      receive(checkGetHeaders)
      List.fill(4)(NewHeaders()).foreach { pbft ! _ }
      store.store(dummyBlock, commits)

      receive(checkNewView(1, 1, viewChanges, dummyBlockHeader, commits))
    }

    "not trigger NewView after 2f received and node is primary without signed commits" ignore {
      val validCommits = (2 to 5)
        .map(node => Commit(node, 1, dummyBlockHeader).sign(keys(node)).require)
        .toList
      val invalidCommit = Commit(6, 1, dummyBlockHeader)
      val commits = validCommits :+ invalidCommit
      val viewChanges = (2 to 6)
        .map(ViewChange(_, 1, dummyBlockHeader, commits))
        .toList

      viewChanges.foreach(send)

      receiveNoMessage()
    }
  }

}

object PbftLogicTest {

  def privKey(b: Byte) = new PrivateKey(dummyList(b, 32), true)
  def privStr(pk: PrivateKey) = PrivateKey.serializeWIF(pk)
  def pubStr(pk: PrivateKey) = pk.getPublic.toByteArray.map("%02x" format _).mkString

  val keys = (0 to 6).map(num => privKey((0x10 + num).toByte))

  val ourPrivateKey = keys(1)

  def signature[M <: PbftPayload[M]](message: M, node: Int)(implicit codec: Codec[M]) = {
    println("hash for " + node + ": " + message.getHash + " message: " + message)
    message.sign(keys(node)).require
  }

  val config =
    s"""
      |hyperledger {
      |  pbft {
      |    bindAddress: "localhost:9999",
      |    nodes: [
      |      {address: "localhost:2345", publicKey: "${pubStr(keys(0))}"}
      |
      |      // our nodes public key => nodeId == 1
      |      {address: "localhost:1234", publicKey: "${pubStr(ourPrivateKey)}"}
      |
      |
      |      {address: "localhost:3456", publicKey: "${pubStr(keys(2))}"}
      |      {address: "localhost:4567", publicKey: "${pubStr(keys(3))}"}
      |      {address: "localhost:5678", publicKey: "${pubStr(keys(4))}"}
      |      {address: "localhost:6789", publicKey: "${pubStr(keys(5))}"}
      |      {address: "localhost:7890", publicKey: "${pubStr(keys(6))}"}
      |    ]
      |    privateKey: "${privStr(ourPrivateKey)}"
      |    protocolTimeoutSeconds: 60
      |    blockFrequencySeconds: 10
      |  }
      |  store {
      |    memory: true
      |  }
      |  mining {
      |    enabled: false
      |    minerAddress: ""
      |  }
      |}
      |akka {
      |  stdout-loglevel: DEBUG
      |  actor {
      |    debug {
      |      receive: on
      |      autoreceive: on
      |      lifecycle: on
      |    }
      |  }
      |}
    """.stripMargin

}
