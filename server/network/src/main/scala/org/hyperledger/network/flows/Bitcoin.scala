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

import _root_.akka.stream._
import _root_.akka.stream.scaladsl._
import akka.actor.ActorRef
import akka.util.ByteString
import org.hyperledger.common._
import org.hyperledger.core.conf.CoreAssemblyFactory
import org.hyperledger.network.InventoryVectorType.{MSG_BLOCK, MSG_TX}
import org.hyperledger.network.Messages._
import org.hyperledger.network._
import org.hyperledger.network.server.PeerInterface
import org.slf4j.LoggerFactory
import scodec.Codec

import scala.concurrent.ExecutionContext

object Bitcoin {
  val LOG = LoggerFactory.getLogger("BitcoinFlow")

  val GET_HEADERS_LIMIT = 2000
  val GET_BLOCKS_LIMIT = 500
  val INV_MAX_SIZE = 50000
  val READ_CONCURRENCY = 2

  def createProcessingGraph(
    peerInterface: PeerInterface[BlockchainMessage],
    hyperLedger: HyperLedger,
    ibd: IBDInterface,
    codec: Codec[BlockchainMessage]
  )
    (implicit ec: ExecutionContext): Flow[ByteString, ByteString, ActorRef] = {
    import HandshakeStage._

    val handshake = HandshakeStage(peerInterface) {
      case VersionMessage(v) => VersionReceived(v)
      case VerackMessage() => VersionAcked
      case RejectMessage(r@Rejection("version", code, reason, data)) => VersionRejected(r)
      case m => NonHandshakeMessage(m)
    }

    val broadcastSource = Source.actorRef[BlockchainMessage](100, OverflowStrategy.dropHead)
    ScodecStage.bidi(codec)
      .atop(BidiFlow.fromGraph(handshake))
      .atop(BidiFlow.fromGraph(InitialBlockDownloadStage(ibd)))
      .joinMat(createProtocolFlow(peerInterface, hyperLedger, broadcastSource))(Keep.right)
  }

  def createProtocolFlow(
    peer: PeerInterface[BlockchainMessage],
    api: HyperLedger,
    broadcastSource: Source[BlockchainMessage, ActorRef])(implicit ec: ExecutionContext) = {

    GraphDSL.create(broadcastSource) { implicit b =>
      broadcast =>
        import GraphDSL.Implicits._

        val protocolMerge = b.add(Merge[BlockchainMessage](7))
        val protocolRoute = b.add(Broadcast[BlockchainMessage](8))

        // format: OFF
        protocolRoute ~> dataFlow(api, peer) ~> protocolMerge
        protocolRoute ~> getHeadersFlow(api) ~> protocolMerge
        protocolRoute ~> getBlocksFlow(api)  ~> protocolMerge
        protocolRoute ~> mempoolFlow(api)    ~> protocolMerge
        protocolRoute ~> getDataFlow(api)    ~> protocolMerge
        protocolRoute ~> pingPongFlow(peer)  ~> protocolMerge
        protocolRoute ~> rejectFlow(api)
        protocolRoute ~> alertFlow(api)
        broadcast                            ~> protocolMerge
        //format: ON

        FlowShape(protocolRoute.in, protocolMerge.out)
    }
  }

  def mempoolFlow(api: HyperLedger) = Flow[BlockchainMessage].collect({ case m: MempoolMessage => () })
    .mapAsync(1) { _ => api.mempool() }
    .mapConcat(_
      .grouped(INV_MAX_SIZE)
      .map(hashes => InvMessage(hashes.map(hash => InventoryVector(MSG_TX, hash))))
      .toList)

  def getDataFlow(api: HyperLedger)(implicit ec: ExecutionContext) = Flow[BlockchainMessage].collect({ case GetDataMessage(inv) => inv })
    .mapConcat(identity)
    .mapAsync(READ_CONCURRENCY) {
      case i@InventoryVector(MSG_TX, hash) =>
        api.fetchTx(new TID(hash)).map(_.map(tx => TxMessage(tx)).getOrElse(NotFoundMessage(List(i))))

      case i@InventoryVector(MSG_BLOCK, hash) =>
        api.fetchBlock(new BID(hash)).map(_.map(b =>
          if (CoreAssemblyFactory.getAssembly.getBlockSignatureConfig.enabled()) SignedBlockMessage(b)
          else BlockMessage(b)).getOrElse(NotFoundMessage(List(i))))
    }

  def getHeadersFlow(api: HyperLedger) = Flow[BlockchainMessage]
    .collect { case g: GetHeadersMessage => g.payload }
    .map { req =>
      val headers = api
        .catchupHeaders(req.locatorHashes, req.hashStop, GET_HEADERS_LIMIT)
        .flatMap(api.fetchHeader)

      if (CoreAssemblyFactory.getAssembly.getBlockSignatureConfig.enabled()) {
        SignedHeadersMessage(headers.map(_.getHeader.asInstanceOf[HeaderWithSignatures]))
      } else {
        HeadersMessage(headers.map(_.getHeader))
      }
    }

  def getBlocksFlow(api: HyperLedger) = Flow[BlockchainMessage] collect { case g: GetBlocksMessage => g.payload } map { req =>
    InvMessage.blocks(api.catchupHeaders(req.locatorHashes, req.hashStop, GET_BLOCKS_LIMIT))
  }

  def rejectFlow(api: HyperLedger) = Flow[BlockchainMessage]
    .collect({ case RejectMessage(r) => api.reject(r) })
    .to(Sink.ignore)

  def alertFlow(api: HyperLedger) = Flow[BlockchainMessage]
    .collect({ case AlertMessage(alert, sig) => api.alert(alert, sig) })
    .to(Sink.ignore)

  def pingPongFlow(peer: PeerInterface[BlockchainMessage]) = Flow[BlockchainMessage].collect {
    case p: PingMessage => p
    case p: PongMessage => p
  }.via(PingPongStage(peer.latencyReport))

  def dataFlow(api: HyperLedger, peer: PeerInterface[BlockchainMessage])(implicit ec: ExecutionContext) =
    Flow[BlockchainMessage]
      .collect({ case m: DataMessage => m })
      .via(Flow.fromGraph(new BlockchainDataStoreStage(api, peer.broadcast)))

  //      val getAddrFlow = placeholder[GetAddrMessage]
  //      val addrFlow = placeholder[AddrMessage]

}
