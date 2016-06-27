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

import org.hyperledger.common._
import org.hyperledger.core.BlockStore.BlockListener
import org.hyperledger.core.{BlockStoredInfo, StoredHeader}
import org.hyperledger.network.InventoryVector
import scodec.Attempt
import scala.collection.JavaConverters._

import scala.concurrent.Future

class TestBlockStore extends PbftBlockstoreInterface {

  import TestBlockStore._

  val genesis: List[(Block, List[Commit])] = List((genesisBlock, List.empty))
  var blockStore: List[(Block, List[Commit])] = genesis
  var blockListener: Option[BlockListener] = None

  override def store(block: Block, commits: List[Commit]) = {
    blockStore = blockStore :+ ((block, commits))
    Future.successful(true)
  }

  override def validateBlock(block: Block) =
    Future.successful(block != invalidBlock)


  override def hasBlock(blockId: BID) =
    Future.successful(blockStore.exists { _._1.getID == blockId })

  override def getCommits(blockId: BID) =
    Attempt.Successful(blockStore.filter { _._1.getID == blockId }.head._2)

  override def headerLocatorHashes = List.empty

  override def getHighestBlock = blockStore.last._1

  def reset() = {
    blockStore = genesis
    blockListener = None
  }

  override def fetchBlock(bid: BID) = Future.successful(None)
  override def fetchTx(tid: TID) = Future.successful(None)

  override def addTransactions(txs: List[Transaction]) =
    Future.successful((List.empty, List.empty))

  override def filterUnkown(inventories: List[InventoryVector]) = Future.successful(Nil)

  override def fetchHeader(h: BID): Option[StoredHeader] = ???

  override def catchUpHeaders(bids: List[BID], hashStop: BID, limit: Int): List[BID] = ???

  override def addHeaders(headers: List[Header]): Future[Unit] = ???

  override def storeCommits(id: BID, commits: List[Commit]): Future[Boolean] = ???

  override def storeBlock(block: Block): Future[(List[BID], List[BlockStoredInfo])] = ???

  override def getHighestHeader: Header = blockStore.last._1.getHeader

  override def removeBlockListener(listener: BlockListener): Unit =
    blockListener = None

  override def addBlockListener(listener: BlockListener): Unit =
    blockListener = Some(listener)

  def callBlockListener(added: BID) =
    blockListener.get.blockStored(new BlockStoredInfo(0, List(added).asJava, List.empty[BID].asJava))

  override def getHeadersAndCommits(locatorHashes: List[BID], hashStop: BID): Attempt[List[(Header, List[Commit])]] = ???

  override def validateAndAdd(header: Header, commits: List[Commit], settings: PbftSettings) = Future.successful(commits)
}

object TestBlockStore {

  def dummyList(elements: Byte, num: Int) = List.fill(num)(elements).toArray
  def dummyHash(elements: Byte) = dummyList(elements, 32)

  val dummyTransaction = new Transaction(1, 0, List[TransactionInput]().asJava, List[TransactionOutput]().asJava)
  val dummyTransactionList = List(dummyTransaction).asJava
  val dummyMerkleRoot = MerkleTree.computeMerkleRoot(dummyTransactionList)

  val genesisPreviousBid = BID.createFromSafeArray(dummyHash(0))
  val genesisBlockHeader = new BitcoinHeader(1, genesisPreviousBid, dummyMerkleRoot, 0, 1, 2)
  val genesisBlock = new Block(genesisBlockHeader, dummyTransactionList)

  val dummyBlockHeader = new BitcoinHeader(1, genesisBlock.getID, dummyMerkleRoot, 0, 1, 2)
  val dummyBlock = new Block(dummyBlockHeader, dummyTransactionList)
  val dummyBlockHeader2 = new BitcoinHeader(1, dummyBlock.getID, dummyMerkleRoot, 0, 1, 2)
  val dummyBlock2 = new Block(dummyBlockHeader2, dummyTransactionList)

  val invalidBlockHeader = new BitcoinHeader(1, genesisBlock.getID, dummyMerkleRoot, 0, 1, 3)
  val invalidBlock = new Block(invalidBlockHeader, dummyTransactionList)

}
