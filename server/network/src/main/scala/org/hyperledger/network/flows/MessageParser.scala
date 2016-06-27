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

import java.nio.ByteOrder

import akka.stream.scaladsl.Flow
import akka.stream.stage._
import akka.util.ByteString
import org.hyperledger.network.Messages
import Messages.BlockchainMessage
import akka.NotUsed
import org.hyperledger.network.Messages

object MessageParser {
  import org.hyperledger.scodec.interop.akka._

  val DEFAULT_MESSAGE_LENGTH_LIMIT = 1024 * 1024 * 32
  def messagePayloadDecoder(magic: Int,
                            maximumMessageLength: Int = DEFAULT_MESSAGE_LENGTH_LIMIT): Flow[ByteString, BlockchainMessage, NotUsed] =
    Flow[ByteString]
      .transform(() => new BitcoinMessageStage(magic, maximumMessageLength))
      .log("Incoming message", _.toString.take(160))
      .named("bitcoinMessageParsing")

  def messagePayloadEncoder(magic: Int) =
    Flow[BlockchainMessage]
      .log("Outgoing message", _.toString.take(160))
      .transform(() => new MessageEncodeStage(magic))

  class MessageEncodeStage(magic: Int) extends PushStage[BlockchainMessage, ByteString] {
    val msgCodec = Messages.messageCodec(magic)
    override def onPush(elem: BlockchainMessage, ctx: Context[ByteString]): SyncDirective = {
      msgCodec.encode(elem).fold(
        e => ctx.fail(new RuntimeException(e.messageWithContext)),
        bits => ctx.push(bits.bytes.toByteString)
      )    }
  }

  private class BitcoinMessageStage(magic: Int, maximumMessageLength: Int) extends PushPullStage[ByteString, BlockchainMessage] {
    val msgCodec = Messages.messageCodec(magic)

    val HEADER_SIZE = 24
    private var buffer = ByteString.empty
    var msgSize: Option[Int] = None

    private def doParse(ctx: Context[BlockchainMessage]): SyncDirective = {

      if (msgSize.isEmpty && buffer.size < HEADER_SIZE) tryPull(ctx)
      else {
        if (msgSize.isEmpty)
          msgSize = Some(parseMsgLength)

        if (msgSize.get > maximumMessageLength) ctx.fail(BitcoinMessageParserException("Maximum message size exceeded"))
        else if (buffer.size < (msgSize.get + HEADER_SIZE)) tryPull(ctx)
        else {
          msgCodec.decode(buffer.toByteVector.bits).fold(
            e => ctx.fail(BitcoinMessageParserException(e.message)),
            b => emitMessage(ctx, b.value))
        }
      }
    }

    private def emitMessage(ctx: Context[BlockchainMessage], msg: BlockchainMessage): SyncDirective = {
      buffer = buffer.drop(msgSize.get + HEADER_SIZE)
      msgSize = None
      if (ctx.isFinishing && buffer.isEmpty) ctx.pushAndFinish(msg)
      else ctx.push(msg)
    }
    def parseMsgLength = buffer.drop(16).iterator.getInt(ByteOrder.LITTLE_ENDIAN)

    override def onPush(chunk: ByteString, ctx: Context[BlockchainMessage]): SyncDirective = {
      buffer ++= chunk
      doParse(ctx)
    }

    override def onPull(ctx: Context[BlockchainMessage]): SyncDirective = {
      doParse(ctx)
    }

    override def onUpstreamFinish(ctx: Context[BlockchainMessage]): TerminationDirective = {
      if (buffer.nonEmpty) ctx.absorbTermination()
      else ctx.finish()
    }

    private def tryPull(ctx: Context[BlockchainMessage]): SyncDirective = {
      if (ctx.isFinishing) ctx.fail(BitcoinMessageParserException(
        "Stream finished but there was a truncated final frame in the buffer"))
      else ctx.pull()
    }

    override def postStop(): Unit = buffer = null
  }

  case class BitcoinMessageParserException(msg: String) extends RuntimeException(msg)

}
