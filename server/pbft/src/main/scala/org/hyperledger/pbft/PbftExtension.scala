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

import java.net.InetSocketAddress

import _root_.akka.actor._
import org.hyperledger.common._
import org.hyperledger.core.BlockStore.BlockListener
import org.hyperledger.core.{ StoredHeader, BlockStoredInfo }
import org.hyperledger.network._
import scodec.{ Err, Attempt }
import scodec.bits.BitVector

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Random
import scalaz.\/

object PbftExtension extends ExtensionId[PbftExtension] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem) = new PbftExtension(system)
  override def get(system: ActorSystem) = apply(system)
  override def lookup(): ExtensionId[_ <: Extension] = PbftExtension
}

class PbftExtension(system: ExtendedActorSystem) extends Extension {
  val settings = PbftSettings.fromConfig(system.settings.config)

  // TODO use the maven version in the wire format version message
  val UA = "/HyperLedger:2.0.0-SNAPSHOT(PBFT)/"
  val PROTOCOL_VERSION = 70002
  val NONCE = Random.nextLong()

  val versionPredicate: Messages.VersionMessage => Boolean = _ => true
  val versionP: Version => String \/ Unit = _ => \/.right(())
  val hyperLedgerCore = HyperLedgerCoreExtension(system)
  var blockStoreConn: PbftBlockstoreInterface = _

  def coreAssembly = hyperLedgerCore.coreAssembly

  blockStoreConn = new PbftBlockstoreConnection(hyperLedgerCore.hyperLedger, settings, PROTOCOL_VERSION, hyperLedgerCore.hyperLedgerExecutionContext)

  def ourVersion(peerAddress: InetSocketAddress) = Version.forNow(
    PROTOCOL_VERSION,
    services = Version.SIMPLE_NODE,
    addrRecv = NetworkAddress.forVersion(Version.SIMPLE_NODE, peerAddress),
    addrFrom = NetworkAddress.forVersion(Version.SIMPLE_NODE, settings.bindAddress),
    nonce = NONCE,
    userAgent = UA,
    startHeight = hyperLedgerCore.api.blockStore.getFullHeight,
    relay = true)

  // format: OFF
  def getHeadersMessage = GetHeadersMessage(BlockDataRequest(PROTOCOL_VERSION,
                                                               blockStoreConn.headerLocatorHashes,
                                                               BID.INVALID))
  // format: ON
}

trait PbftBlockstoreInterface {
  def fetchTx(tid: TID): Future[Option[Transaction]]
  def fetchHeader(h: BID): Option[StoredHeader]
  def fetchBlock(h: BID): Future[Option[Block]]
  def filterUnkown(inventories: List[InventoryVector]): Future[List[InventoryVector]]
  def hasBlock(blockId: BID): Future[Boolean]
  def headerLocatorHashes: List[BID]
  def getHighestBlock: Block
  def getHighestHeader: Header
  def addTransactions(txs: List[Transaction]): Future[(List[TID], List[TID])]
  def addHeaders(headers: List[Header]): Future[Unit]
  def addBlockListener(listener: BlockListener): Unit
  def removeBlockListener(listener: BlockListener): Unit
  def catchUpHeaders(bids: List[BID], hashStop: BID, limit: Int): List[BID]
  def getHeadersAndCommits(locatorHashes: List[BID], hashStop: BID): Attempt[List[(Header, List[Commit])]]

  def store(block: Block, commits: List[Commit]): Future[Boolean]
  def storeBlock(block: Block): Future[(List[BID], List[BlockStoredInfo])]
  def storeCommits(id: BID, commits: List[Commit]): Future[Boolean]
  def getCommits(blockId: BID): Attempt[List[Commit]]
  def validateBlock(block: Block): Future[Boolean]
  def validateAndAdd(header: Header, commits: List[Commit], settings: PbftSettings): Future[List[Commit]]
}

class PbftBlockstoreConnection(hyperLedger: HyperLedger,
  settings: PbftSettings,
  protocolVersion: Int,
  implicit val executionContext: ExecutionContext)
  extends PbftBlockstoreInterface {

  def store(block: Block, commits: List[Commit]) = {
    storeBlock(block).flatMap { x => storeCommits(block.getID, commits) }
  }

  def storeBlock(block: Block) = {
    hyperLedger.addBlocks(List(block))
  }

  def storeCommits(id: BID, commits: List[Commit]) = {
    encodeCommits(commits).map { encoded => hyperLedger.blockStore.addMiscData(id, encoded.toByteArray) }
  }

  def encodeCommits(commits: List[Commit]) = {
    codecs.varIntSizeSeq(Commit.codec).encode(commits).fold(
      err => Future.failed(new HyperLedgerException(err.message)),
      encoded => Future.successful(encoded))
  }

  def validateAndAdd(header: Header, commits: List[Commit], settings: PbftSettings) = {
    import SignatureValidator._

    val (bad, _) = split(validateCommits(commits).run(settings))
    if (bad.nonEmpty) {
      Future.failed(new HyperLedgerException(bad.head._1.message))
    } else if (!commits.forall(header.getID == _.blockHeader.getID)) {
      Future.failed(new HyperLedgerException(s"Commits not for ${header.getID}"))
    } else {
      addHeaders(List(header))
        .flatMap { _ => storeCommits(header.getID, commits) }
        .map { _ => commits }
    }
  }

  def fetchTx(tid: TID) = hyperLedger.fetchTx(tid)
  def fetchHeader(h: BID) = hyperLedger.fetchHeader(h)
  def fetchBlock(h: BID) = hyperLedger.fetchBlock(h)
  def filterUnkown(inventories: List[InventoryVector]) = hyperLedger.filterUnknown(inventories)
  def hasBlock(blockId: BID) = hyperLedger.hasBlock(blockId)
  def headerLocatorHashes: List[BID] = hyperLedger.headerLocatorHashes()
  def getHighestBlock = hyperLedger.blockStore.getHighestBlock
  def getHighestHeader = hyperLedger.blockStore.getHighestHeader
  def addTransactions(txs: List[Transaction]) = hyperLedger.addTransactions(txs)
  def addHeaders(headers: List[Header]) = hyperLedger.addHeaders(headers)
  def addBlockListener(listener: BlockListener) = hyperLedger.blockStore.addBlockListener(listener)
  def removeBlockListener(listener: BlockListener) = hyperLedger.blockStore.removeBlockListener(listener)
  def catchUpHeaders(bids: List[BID], hashStop: BID, limit: Int) = hyperLedger.catchupHeaders(bids, hashStop, limit)

  def getHeadersAndCommits(locatorHashes: List[BID], hashStop: BID) = {
    val headersAndCommits = catchUpHeaders(locatorHashes, hashStop, Int.MaxValue)
      .flatMap(fetchHeader)
      .map { header => (header, hyperLedger.blockStore.getMiscData(header.getID)) }
      .filter(_._2 != null)
      .map { case (header, misc) =>
        val commits = codecs.varIntSizeSeq(Commit.codec).decode(BitVector(misc)) map { _.value }
        (header, commits)
      }
      .map(attemptTuple)
    attemptSequence(headersAndCommits)
  }

  def attemptSequence[T](s: List[Attempt[T]]): Attempt[List[T]] = s.partition(_.isFailure) match {
    case (Nil, successes) =>
      Attempt.Successful(for (Attempt.Successful(item) <- successes) yield item)
    case (errors, _) =>
      val Attempt.Failure(firstError) = errors.head
      Attempt.failure(firstError)
  }

  def attemptTuple[A, B](t: (A, Attempt[B])): Attempt[(A, B)] = t._2 match {
    case Attempt.Successful(bs) => Attempt.Successful((t._1, bs))
    case e: Attempt.Failure     => e
  }

  def getCommits(blockId: BID): Attempt[List[Commit]] = {
    val encoded = Option(hyperLedger.blockStore.getMiscData(blockId))
    encoded match {
      case Some(data) =>
        codecs.varIntSizeSeq(Commit.codec).decode(BitVector(data)) map { _.value }
      case None =>
        Attempt.failure(Err(s"Commits not stored for $blockId"))
    }
  }

  def validateBlock(block: Block): Future[Boolean] = Future(hyperLedger.blockStore.validateBlock(block))

}
