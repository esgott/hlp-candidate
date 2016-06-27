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

import akka.actor.ActorSystem
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.hyperledger.pbft.MinerTest._
import org.hyperledger.pbft.PbftHandler.BlockMined
import org.hyperledger.pbft.PbftMiner.StartMining
import org.scalatest.{ Matchers, WordSpecLike }

class MinerTest extends TestKit(ActorSystem("test", ConfigFactory.parseString(config)))
  with WordSpecLike with Matchers with ImplicitSender {

  import scala.concurrent.duration._

  "The PbftMiner" should {
    "be created with the configuration" ignore {
      val extension = PbftExtension(system)
      val miner = system.actorOf(PbftMiner.props())
      miner ! StartMining()
      expectMsgClass(15 seconds, classOf[BlockMined])
    }
  }

}

object MinerTest {
  val config =
    s"""
       |hyperledger {
       |  pbft {
       |    miner-dispatcher {
       |      type = Dispatcher
       |      executor = "thread-pool-executor"
       |      thread-pool-executor {
       |        core-pool-size-min = 1
       |        core-pool-size-max = 1
       |      }
       |    }
       |
       |    nodes = [
       |      {address: "localhost:1234", publicKey: "0279f6a3d862c7b367524e7d0875fb85314b4f960ceeb2526a2ea0e8585dc8ff69"}
       |      {address: "localhost:2345", publicKey: "0385ea1509ab2dfbb375ea5e41819ebaf49127df9fcec70a93b84b0aa57f6b4642"}
       |    ]
       |    privateKey: "L3i1bb4tPemhifqwqa9s5hriARpP4s1edkLcJ6xrsqLnnr3jGLcL"
       |    bindAddress: "localhost:9999",
       |    protocolTimeoutSeconds: 60
       |    blockFrequencySeconds: 1
       |  }
       |
       |  store {
       |    memory: true
       |  }
       |  mining {
       |    enabled: false
       |    minerAddress: "1CNABTVtwxFQBTvazuGfxhT87sfssmFbdE"
       |  }
       |  blockchain {
       |    chain: "regtest"
       |  }
       |}
     """.stripMargin
}
