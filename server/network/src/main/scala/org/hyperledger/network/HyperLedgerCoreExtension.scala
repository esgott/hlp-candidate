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
package org.hyperledger.network

import akka.actor._
import org.hyperledger.common._
import org.hyperledger.core._
import org.hyperledger.core.bitcoin.BitcoinMiner
import org.hyperledger.core.conf.{CoreAssembly, CoreAssemblyFactory}
import scodec.bits.BitVector

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scalaz.Scalaz._

object HyperLedgerCoreExtension extends ExtensionId[HyperLedgerCoreExtension] with ExtensionIdProvider {
  override def get(system: ActorSystem): HyperLedgerCoreExtension = apply(system)

  override def createExtension(system: ExtendedActorSystem) = new HyperLedgerCoreExtension(system)

  override def lookup() = HyperLedgerCoreExtension
}

class HyperLedgerCoreExtension(system: ExtendedActorSystem) extends Extension {
  val config = system.settings.config


  var hyperLedger: HyperLedger = _

  private val settings = HyperLedgerExtensionSettings(system.settings.config)
  val hyperLedgerExecutionContext = system.dispatchers.lookup(settings.dispatcherName)

  CoreAssemblyFactory.reset()
  CoreAssemblyFactory.initialize(system.settings.config)
  var coreAssembly = CoreAssemblyFactory.getAssembly
  coreAssembly.start()

  hyperLedger = new HyperLedgerAkka(system, coreAssembly, hyperLedgerExecutionContext)

  system.registerOnTermination {
    coreAssembly.stop()
  }

  @deprecated
  def initialize(): Unit = ()

  def api = {
    if (hyperLedger == null)
      throw new IllegalStateException("HyperLedgerExtension is not initialized")
    hyperLedger
  }
}

trait HyperLedger {
  def blockStore: BlockStore
  def clientEventQueue: ClientEventQueue
  def headerLocatorHashes(): List[BID]
  def missingBlocks(i: Int): List[BID]
  def catchupHeaders(bids: List[BID], hashStop: BID, limit: Int): List[BID]
  def hasBlock(h: BID): Future[Boolean]
  def hasTransaction(h: TID): Future[Boolean]
  def filterUnknown(inventories: List[InventoryVector]): Future[List[InventoryVector]]
  def addHeaders(headers: List[Header]): Future[Unit]
  def addBlocks(blocks: List[Block]): Future[(List[BID], List[BlockStoredInfo])]

  /**
   * Add all transactions to the mempool. The returned tuple contains TIDs for failed addition (first member) and
   * TIDs for successfully added transactions (second member)
   */
  def addTransactions(txs: List[Transaction]): Future[(List[TID], List[TID])]
  def fetchTx(h: TID): Future[Option[Transaction]]
  def fetchBlock(h: BID): Future[Option[Block]]
  def fetchHeader(h: BID): Option[StoredHeader]
  def mempool(): Future[List[TID]]
  def alert(alert: Alert, sig: BitVector): Unit
  def reject(r: Rejection): Unit
  def getMiner: Option[BitcoinMiner]
}

/**
 * API for accessing HyperLedger functionality in a Scala and Akka friendly way
 */
class HyperLedgerAkka(system: ActorSystem, core: CoreAssembly, implicit val executionContext: ExecutionContext) extends HyperLedger {
  val PROTOCOL_VERSION = 70002

  val blockStore = core.getBlockStore
  val clientEventQueue = core.getClientEventQueue

  def catchupHeaders(bids: List[BID], hashStop: BID, limit: Int = Integer.MAX_VALUE): List[BID] =
    blockStore.catchUpHeaders(bids.asJava, limit).asScala
      .span(_ != hashStop)
      .rightMap(_.take(1))
      .fold(_ ++ _)
      .toList

  def headerLocatorHashes(): List[BID] = blockStore.getHeaderLocator.asScala.toList

  def missingBlocks(i: Int): List[BID] = blockStore.getMissingBlocks(i).asScala.toList

  def hasBlock(h: BID): Future[Boolean] = Future(blockStore.hasBlock(h))

  def hasTransaction(h: TID): Future[Boolean] = Future(blockStore.hasTransaction(h))

  def filterUnknown(inventories: List[InventoryVector]) = Future.traverse(inventories) {
    case inv if inv.typ == InventoryVectorType.MSG_BLOCK => hasBlock(new BID(inv.hash)).map(inv -> _)
    case inv if inv.typ == InventoryVectorType.MSG_TX    => hasTransaction(new TID(inv.hash)).map(inv -> _)
  }.map(_.collect { case (inv, known) if !known => inv })

  def addHeaders(headers: List[Header]) = Future(headers foreach blockStore.addHeader)

  def addBlocks(blocks: List[Block]): Future[(List[BID], List[BlockStoredInfo])] = Future {
    blocks.foldMap { block =>
      try {
        (Nil, blockStore.addBlock(block) :: Nil)
      } catch {
        case _: LoggedHyperLedgerException => (block.getID :: Nil, Nil)
        case e: Throwable                  => throw e
      }
    }
  }

  /**
   * Add all transactions to the mempool. The returned tuple contains TIDs for failed addition (first member) and
   * TIDs for successfully added transactions (second member)
   */
  def addTransactions(txs: List[Transaction]): Future[(List[TID], List[TID])] = Future {
    txs.foldMap { tx =>
      try {
        (Nil, blockStore.addTransaction(tx).getID :: Nil)
      } catch {
        case _: LoggedHyperLedgerException => (tx.getID :: Nil, Nil)
        case e: Throwable                  => throw e
      }
    }
  }

  def fetchTx(h: TID): Future[Option[Transaction]] = Future(Option(blockStore.getTransaction(h)))

  def fetchBlock(h: BID): Future[Option[Block]] = Future(Option(blockStore.getBlock(h)))

  def fetchHeader(h: BID): Option[StoredHeader] = Option(blockStore.getHeader(h))

  def mempool(): Future[List[TID]] = Future(blockStore.getMempoolContent.asScala.toList.map(_.getID))

  def alert(a: Alert, sig: BitVector) = println(s"Alert received $a")

  def reject(r: Rejection): Unit = {
    if ((r.message == "tx" || r.message == "block" || r.message == "sigblock") && r.extraData.isDefined && r.extraData.get.size == 32) {
      blockStore.rejectedByPeer(r.message, new Hash(r.extraData.get.toArray), r.ccode.value)
    }
  }

  def getMiner: Option[BitcoinMiner] = Option(core.getMiner)
}

