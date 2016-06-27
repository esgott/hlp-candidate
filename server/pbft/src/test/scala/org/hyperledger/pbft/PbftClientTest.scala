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

import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern.ask
import akka.testkit.{ DefaultTimeout, TestFSMRef, TestKit }
import com.typesafe.config.ConfigFactory
import org.hyperledger.common.Header
import org.hyperledger.pbft.CommitCounter.Finished
import org.hyperledger.pbft.PbftClientTest._
import org.scalatest._

import scala.concurrent.Await

class PbftClientTest extends TestKit(ActorSystem("test", ConfigFactory.load(ConfigFactory.parseString(config))))
  with DefaultTimeout with FlatSpecLike with Matchers with BeforeAndAfterEach {

  import CommitCounter.getBlockMessage
  import TestBlockStore._

  val extension = PbftExtension(system)
  val store = new TestBlockStore()
  extension.blockStoreConn = store

  val f = 2

  override def beforeEach() = {
    store.reset()
  }

  def send(msg: Any)(implicit client: ActorRef) = {
    sendReceive(msg, PbftClient.emptyResponse)
  }

  def sendReceive(msg: Any, response: Any)(implicit client: ActorRef) = {
    val result = Await.result(client ? msg, timeout.duration)
    result shouldBe response
  }

  def sendNoResponse(msg: Any)(implicit client: ActorRef) = {
    client ! msg
  }

  def commitMsg(node: Int, header: Header) = CommitMessage(Commit(node, 0, header))

  "PbftClient" should "ask for the block after 2f+1 Commit messages" in {
    implicit val client = TestFSMRef(new PbftClient())
    val commits = (1 to 2*f+1) map { node => commitMsg(node, dummyBlockHeader) }

    commits take 2*f foreach send
    client.stateData.counters(dummyBlockHeader.getID) shouldNot be(None)

    sendReceive(commits.last, getBlockMessage(dummyBlockHeader.getID))
  }

  // flaky test, ignore until PROD-210 is done
  ignore should "not accept Commit messages after block accepted" in {
    implicit val client = TestFSMRef(new PbftClient())
    val commits = (1 to 2*f+1) map { node => commitMsg(node, dummyBlockHeader) }

    commits take 2*f foreach send
    sendReceive(commits.last, getBlockMessage(dummyBlockHeader.getID))

    send(commitMsg(2*f+2, dummyBlockHeader))
  }

  it should "not accept Commit messages after consensus times out" in {
    implicit val client = TestFSMRef(new PbftClient())
    val commits = (1 to 2*f+1) map { node => commitMsg(node, dummyBlockHeader) }

    commits take 2*f foreach send
    sendNoResponse(Finished(dummyBlockHeader.getID))
    send(commits.last)
    client.stateData.counters.get(dummyBlockHeader.getID) shouldBe Some(None)
  }

  it should "remove the CommitCounter after block accepted" in {
    implicit val client = TestFSMRef(new PbftClient())
    val commits = (1 to 2*f+1) map { node => commitMsg(node, dummyBlockHeader) }

    commits take 2*f foreach send
    store.store(dummyBlock, commits.map(_.payload).toList)
    store.callBlockListener(dummyBlock.getID)
    send(commits.last)
    client.stateData.counters.get(dummyBlockHeader.getID) shouldBe None
  }

  it should "create two CommitCounters for two different blocks" in {
    implicit val client = TestFSMRef(new PbftClient())
    send(commitMsg(0, dummyBlockHeader))
    send(commitMsg(0, dummyBlockHeader2))
    client.stateData.counters.size shouldBe 2
  }

}

object PbftClientTest {

  import PbftLogicTest._

  val config =
    s"""
       |hyperledger {
       | pbft {
       |   bindAddress: "localhost:9999",
       |   nodes: [
       |     {address: "localhost:2345", publicKey: "${pubStr(keys(0))}"}
       |     {address: "localhost:1234", publicKey: "${pubStr(keys(1))}"}
       |     {address: "localhost:3456", publicKey: "${pubStr(keys(2))}"}
       |     {address: "localhost:4567", publicKey: "${pubStr(keys(3))}"}
       |     {address: "localhost:5678", publicKey: "${pubStr(keys(4))}"}
       |     {address: "localhost:6789", publicKey: "${pubStr(keys(5))}"}
       |     {address: "localhost:7890", publicKey: "${pubStr(keys(6))}"}
       |   ]
       |   protocolTimeoutSeconds: 60
       |   blockFrequencySeconds: 10
       | }
       | store {
       |   memory: true
       | }
       | mining {
       |   enabled: false
       |   minerAddress: ""
       | }
       |}
      """.stripMargin
}
