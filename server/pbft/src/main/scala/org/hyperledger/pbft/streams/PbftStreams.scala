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

import akka.actor.ActorRef
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import org.hyperledger.common.{Block, Transaction}
import org.hyperledger.network.Implicits._
import org.hyperledger.network.InventoryVectorType.{MSG_BLOCK, MSG_TX}
import org.hyperledger.network._
import org.hyperledger.network.flows.ScodecStage
import org.hyperledger.pbft.PbftHandler.UpdateViewSeq
import org.hyperledger.pbft.RecoveryActor.NewHeaders
import org.hyperledger.pbft._
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scalaz.\/

object PbftStreams {
  import scala.concurrent.duration._
  implicit val timeout: Timeout = 1 seconds

  val LOG = LoggerFactory.getLogger("PbftStreams")

  val NETWORK_MAGIC = 1
  val HEADERS_MAX_SIZE = 1000

  /**
    * Creates the akka stream flow for the PBFT server. The materialized value for the returned flow is:
    * - A Future of Version, which will be completed after a successful handshake, and will return with the peer's Version
    * - An ActorRef, which can be used to send PbftMessages to the peer
    *
    * The `broadcaster` parameter, which can be used by this logic to broadcast messages to other peers is a hack. It
    * is needed because we can't listen to mempool transaction events, because ClientEventQueue only contains one queue,
    * which is consumed by the BCSAPIServer to send out TransactionEvents. If ClientEventQueue was an EventBus like
    * functionality, we could use that at the PbftServer level to broadcast transaction events to peers.
    * @return
    */
  def createFlow(settings: PbftSettings,
    store: PbftBlockstoreInterface,
    versionP: Version => String \/ Unit,
    broadcaster: ActorRef,
    handler: ActorRef,
    ourVersion: Version,
    outbound: Boolean)(implicit ec: ExecutionContext): Flow[ByteString, ByteString, (Future[Version], ActorRef)] = {
    val handshake = new HandshakeStage(ourVersion, versionP, outbound)

    ScodecStage.bidi(PbftMessage.messageCodec(NETWORK_MAGIC))
      .atopMat(BidiFlow.fromGraph(handshake))(Keep.right)
      .joinMat(createProtocolFlow(settings, store, broadcaster, handler))(Keep.both)
  }

  type PbftMessagePF = PartialFunction[PbftMessage, PbftMessage]

  val txInv: PartialFunction[PbftMessage, InvMessage] = {
    case InvMessage(i) if i.exists(_.typ == MSG_TX) => InvMessage(i.filter(_.typ == MSG_TX))
  }
  val txAsInv: PartialFunction[PbftMessage, InvMessage] = {
    case TxMessage(tx) => InvMessage(List(InventoryVector.tx(tx.getID)))
  }

  val allButTxIn: PbftMessagePF = {
    case InvMessage(i) if i.exists(_.typ != MSG_TX) => InvMessage(i.filter(_.typ != MSG_TX))
    case m => m
  }

  val consensusMessages: PbftMessagePF = {
    case m: PrePrepareMessage => m
    case m: PrepareMessage    => m
    case m: CommitMessage     => m
    case m: ViewChangeMessage => m
    case m: NewViewMessage    => m
  }

  def createProtocolFlow(settings: PbftSettings,
    store: PbftBlockstoreInterface,
    broadcastOut: ActorRef,
    handler: ActorRef)(implicit ec: ExecutionContext): Graph[FlowShape[PbftMessage, PbftMessage], ActorRef] = {
    val broadcastSource = Source.actorRef[PbftMessage](100, OverflowStrategy.dropHead)
    GraphDSL.create(broadcastSource) { implicit b =>
      broadcast =>
        import GraphDSL.Implicits._

        val consensusFlow = Flow[PbftMessage].collect(consensusMessages)
          .mapAsync(1)(m => (handler ? m).mapTo[List[PbftMessage]])
          .mapConcat[PbftMessage](identity)

        val txFlow = Flow[PbftMessage].collect { case tx: TxMessage => tx }
          .mapAsync(1) { tm =>
            val result = store.addTransactions(List(tm.payload))
            // broadcast successful store
            result.onSuccess[Unit] {
              case (failure, success) if success.nonEmpty => broadcastOut ! InvMessage(success.map(InventoryVector.tx))
            }

            // Map failed store operations to RejectMessages. If tx is stored without failure this will be an empty list
            result.map(_._1.map(Rejection.invalidTx("invalid-tx")).map(RejectMessage))
          }.mapConcat(identity)

        val invFlow = Flow[PbftMessage].collect { case m: InvMessage => m }
          .mapAsync(1)(m => store.filterUnkown(m.payload).map(GetDataMessage))

        val getHeadersFlow = Flow[PbftMessage]
          .collect {
            case GetHeadersMessage(BlockDataRequest(_, locatorHashes, hashStop)) => store.getHeadersAndCommits(locatorHashes, hashStop)
          }.mapConcat(_.fold(
            err => {
              LOG.error(s"Error when reading commits: ${err.message}")
              List.empty
            },
            list => {
              val headersAndCommits = list.take(HEADERS_MAX_SIZE).map((HeaderAndCommit.apply _).tupled)
              List(HeadersMessage(headersAndCommits))
            }))

        val headersFlow = Flow[PbftMessage]
          .mapAsync(1) {
            case m: HeadersMessage =>
              val invVectors = m.payload.map { item =>
                Option(store.fetchHeader(item.header.getID)) match {
                  case None =>
                    store.validateAndAdd(item.header, item.commits, settings)
                      .flatMap { commits =>
                        handler ! UpdateViewSeq(commits.head.viewSeq)
                        val id = commits.head.blockHeader.getID
                        store.hasBlock(id) map { hasIt => (hasIt, id) }
                      }.map { hasIt =>
                        if (hasIt._1) None
                        else Some(InventoryVector(MSG_BLOCK, hasIt._2))
                      }
                  case Some(_) =>
                    Future.successful(None)
                }

              }
              Future.sequence(invVectors).map { ivs =>
                handler ! NewHeaders()
                val invVectors = ivs.flatten
                if (invVectors.nonEmpty) {
                  List(GetDataMessage(invVectors))
                } else {
                  List.empty
                }
              }
            case _ => Future.successful(Nil)
          }.mapConcat(identity)

        val blockFlow = Flow[PbftMessage].collect { case BlockMessage(block) => block }
          .mapAsync(1)( block => for {
              hasBlock <- store.hasBlock(block.getID) if !hasBlock
              valid <- store.validateBlock(block) if valid
              stored <- store.storeBlock(block)
              (failed, storedBlocks) = stored
            } yield failed)
          .mapConcat(_.map(failedBid => RejectMessage(Rejection.invalidBlock("InvalidBlock")(failedBid))))

        val getDataFlow = Flow[PbftMessage]
          .mapConcat {
            case m: GetDataMessage => m.payload filter { msg => msg.typ == MSG_TX || msg.typ == MSG_BLOCK }
            case _                 => Nil
          }
          .mapAsync(1) {
            case InventoryVector(MSG_TX, id)    => store.fetchTx(id.toTID)
            case InventoryVector(MSG_BLOCK, id) => store.fetchBlock(id.toBID)
            case _                              => Future.successful(None)
          }
          .collect {
            case Some(t: Transaction) => TxMessage(t)
            case Some(b: Block)       => BlockMessage(b)
          }

        val verifySig = b.add(Flow[PbftMessage].transform(() => new VerifySignatureStage(settings)))

        val inputRoute = b.add(Broadcast[PbftMessage](8))
        val outMerge = b.add(Merge[PbftMessage](9, eagerComplete = true))

        val broadcastSplit = b.add(Broadcast[PbftMessage](2))
        val txInvCache = Flow[PbftMessage].collect(txInv orElse txAsInv)
        val txInvBroadcast = Flow[PbftMessage].collect(txInv)
        val nonTxInv = Flow[PbftMessage].collect(allButTxIn)
        val txTracker = b.add(InventoryTracker(100))

        // format: OFF
        //inputLogger ~>
        verifySig ~> inputRoute     ~> consensusFlow                                     ~> outMerge
                     inputRoute     ~> invFlow                                           ~> outMerge
                     inputRoute     ~> getDataFlow                                       ~> outMerge
                     inputRoute     ~> getHeadersFlow                                    ~> outMerge
                     inputRoute     ~> headersFlow                                       ~> outMerge
                     inputRoute     ~> txFlow                                            ~> outMerge //~> outputLogger
                     inputRoute     ~> blockFlow                                         ~> outMerge
                     inputRoute     ~> txInvCache       ~> txTracker.peerInput
        broadcast ~> broadcastSplit ~> txInvBroadcast   ~> txTracker.broadcastInput
                                                           txTracker.out                 ~> outMerge
                     broadcastSplit ~> nonTxInv                                          ~> outMerge

        // format: ON

        //FlowShape(inputLogger.in, outputLogger.out)
        FlowShape(verifySig.in, outMerge.out)
    }
  }

}
