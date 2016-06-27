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

import akka.actor.ActorRef
import akka.stream._
import akka.stream.stage.{ AsyncCallback, GraphStage, GraphStageLogic, InHandler }
import org.hyperledger.common.{ BID, Header }
import org.hyperledger.network.IBDInterface
import org.hyperledger.network.Messages._
import org.hyperledger.network.flows.InitialBlockDownloader._
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }

object InitialBlockDownloadStage {
  val LOG = LoggerFactory.getLogger(classOf[InitialBlockDownloadStage])

  def apply(ibdIF: IBDInterface)(implicit ec: ExecutionContext) = new InitialBlockDownloadStage(ibdIF, ec)
}
/**
 * A flow stage which normally let all data messages to pass through. In the Initial Block Download phase it ignores
 * all messages except BlockMessage, which is sent to the blockDownloader actor.
 *
 * @see InitialBlockDownloader
 */
class InitialBlockDownloadStage(ibdIF: IBDInterface, implicit val ec: ExecutionContext)
  extends GraphStage[BidiShape[BlockchainMessage, BlockchainMessage, BlockchainMessage, BlockchainMessage]] {

  import InitialBlockDownloadStage._

  val headersExtract: PartialFunction[BlockchainMessage, List[Header]] = {
    case HeadersMessage(headers)       => headers
    case SignedHeadersMessage(headers) => headers
  }

  val IBD_THRESHOLD = 144
  val HEADERS_MAX_SIZE = 2000

  val protoIn = Inlet[BlockchainMessage]("proto.in")
  val protoOut = Outlet[BlockchainMessage]("proto.out")
  val appIn = Inlet[BlockchainMessage]("app.in")
  val appOut = Outlet[BlockchainMessage]("app.out")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var self: ActorRef = _
    var inIBD = false
    var blockDownloader: ActorRef = _
    var headersCallback: AsyncCallback[(Int, List[BlockchainMessage])] = _
    var ibdRequests = Set.empty[BID]

    override def preStart(): Unit = {
      self = getStageActor {
        case (_, RequestBlockDownloadBatch(bids)) =>
          LOG.debug(s"Requesting batch download $bids")
          inIBD = true
          ibdRequests ++= bids
          emit(protoOut, GetDataMessage.blocks(bids))
        case (_, ContinueAsNormal) if inIBD =>
          LOG.debug("Initial Block Download is over")
          inIBD = false
        case (_, ContinueAsNormal) => // ignore
        case m                     => LOG.warn("Unexpected message $m")
      }.ref
      blockDownloader = ibdIF.registerForInitialBlockDownload(self)
      headersCallback = getAsyncCallback[(Int, List[BlockchainMessage])] {
        case ((received, reply)) =>
          if (received > IBD_THRESHOLD)
            inIBD = true

          if (inIBD && received < HEADERS_MAX_SIZE)
            blockDownloader ! CheckMissingHeaders

          if (reply.nonEmpty) emitMultiple(protoOut, reply, () => tryPull(protoIn))
          else tryPull(protoIn)
      }
      tryPull(protoIn)
      tryPull(appIn)
    }

    val headerHandlers = headersExtract.andThen { headers =>
      ibdIF.addHeaders(headers).flatMap { _ =>
        LOG.debug(s"Received ${headers.size} headers")
        if (headers.size == HEADERS_MAX_SIZE) {
          Future.successful((headers.size, List(ibdIF.createGetHeadersRequest())))
        } else if (inIBD) {
          Future.successful((headers.size, Nil))
        } else {
          val missingHeaderTasks = headers.map(header => ibdIF.hasBlock(header.getID).map(_ -> header.getID))
          Future.sequence(missingHeaderTasks).map { tuples =>
            val reply = tuples.filterNot(_._1).map(_._2) match {
              case Nil            => Nil
              case missingHeaders => List(GetDataMessage.blocks(missingHeaders))
            }
            (headers.size, reply)
          }
        }
      }.recover({ case _ => (0, Nil) }).foreach(headersCallback.invoke)
    }

    val messageHandler: PartialFunction[BlockchainMessage, Unit] = {
      case BlockMessage(block) if inIBD && ibdRequests(block.getID) =>
        LOG.debug(s"Received block ${block.getID}")
        ibdRequests -= block.getID
        blockDownloader ! CompleteBlockDownload(block)
        tryPull(protoIn)
      case m: DataMessage if inIBD => tryPull(protoIn)
      case m                       => emit(appOut, m, () => tryPull(protoIn))
    }

    passAlong(appIn, protoOut)
    setHandler(appOut, eagerTerminateOutput)
    setHandler(protoIn, new InHandler {
      override def onPush(): Unit = (headerHandlers orElse messageHandler)(grab(protoIn))
    })
    setHandler(protoOut, eagerTerminateOutput)

  }

  override val shape = new BidiShape(protoIn, appOut, appIn, protoOut)
}
