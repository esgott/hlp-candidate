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
package org.hyperledger.network.server

import java.util.Collections

import org.hyperledger.common._
import org.scalatest._

import scala.collection.JavaConverters._

class TransactionDownloadStateSpec extends FunSpec with Matchers {
  case class InputStub(sourceHash: TID) extends TransactionInput(new Outpoint(sourceHash, 0), 0, Script.EMPTY) {
    override def getSourceTransactionID = sourceHash
  }
  case class TxStub(hash: TID, dependencies: List[TID]) extends Transaction(1, 1, dependencies.map(InputStub).asJava, Collections.emptyList(), hash) {
    override def getID: TID = new TID(hash)
    override def equals(obj: scala.Any): Boolean = obj.isInstanceOf[TxStub] && obj.asInstanceOf[TxStub].hash == hash
    override def toString = hash.toString
  }

  val hash1 = new TID(Hash.hash(Array[Byte](1)))
  val hash2 = new TID(Hash.hash(Array[Byte](2)))
  val hash3 = new TID(Hash.hash(Array[Byte](3)))
  val hash4 = new TID(Hash.hash(Array[Byte](4)))
  val hash5 = new TID(Hash.hash(Array[Byte](5)))
  val hash6 = new TID(Hash.hash(Array[Byte](6)))
  val hashes = hash1 :: hash2 :: hash3 :: hash4 :: hash5 :: hash6 :: Nil

  val hashx = new TID(Hash.hash(Array[Byte]('x')))

  import TransactionDownloadState._

  describe("TransactionDownloadState2") {
    describe("if no double-spend occurs") {
      /*
                  1
              /---+---\
              2       3
          /---+---\   |
          4       5   6
       */
      val tx1 = TxStub(hash1, hash2 :: hash3 :: Nil)
      val tx2 = TxStub(hash2, hash4 :: hash5 :: Nil)
      val tx3 = TxStub(hash3, hash6 :: Nil)
      val tx4 = TxStub(hash4, Nil)
      val tx5 = TxStub(hash5, Nil)
      val tx6 = TxStub(hash6, Nil)

      describe("if it is empty") {
        val s = TransactionDownloadState.empty
        it("should add a received transaction dependencies as downloadable") {
          val (s1, downloads) = s.transactionReceived(tx1, List(hash2, hash3))
          s1 shouldBe TransactionDownloadState(Map(hash1 -> tx1), Map(hash2 -> hash1, hash3 -> hash1))
          downloads shouldBe DownloadTransactions(hash2 :: hash3 :: Nil)
          List(hash1, hash2, hash3).map(s1.isKnown).forall(identity) shouldBe true
        }
        it("should be still empty if a transaction stored by others") {
          s.transactionStored(hash1) shouldBe ((s, Noop))
        }
      }

      describe("if a common spending tx is missing from the known txs") {
        val s = TransactionDownloadState(
          Map(hash2 -> tx2, hash3 -> tx3),
          Map(hash4 -> hash2, hash5 -> hash2, hash6 -> hash3))
        it("should receive the common spending tx and make all others depends on that") {
          val (s1, _) = s.transactionReceived(tx1, hash2 :: hash3 :: Nil)
          s1 shouldBe TransactionDownloadState(
            Map(hash1 -> tx1, hash2 -> tx2, hash3 -> tx3),
            Map(hash4 -> hash1, hash5 -> hash1, hash6 -> hash1))
        }
      }

      describe("if it has some txs with pending dependencies") {
        val s = TransactionDownloadState(
          Map(hash1 -> tx1, hash2 -> tx2),
          Map(hash4 -> hash1, hash5 -> hash1, hash3 -> hash1))
        it("should forget pending download if it was stored elsewhere") {
          val (s1, Noop) = s.transactionStored(hash5)
          s1 shouldBe TransactionDownloadState(
            Map(hash1 -> tx1, hash2 -> tx2),
            Map(hash4 -> hash1, hash3 -> hash1))
        }
        it("should remove tx stored elsewhere as well as its inputs") {
          val (s1, Noop) = s.transactionStored(hash2)
          s1 shouldBe TransactionDownloadState(Map(hash1 -> tx1), Map(hash3 -> hash1))
        }
        it("should process downloaded dependency with more missing deps") {
          val (s1, DownloadTransactions(toDownload)) = s.transactionReceived(tx3, hash6 :: Nil)
          toDownload shouldBe hash6 :: Nil
          s1 shouldBe TransactionDownloadState(
            Map(hash1 -> tx1, hash2 -> tx2, hash3 -> tx3),
            Map(hash4 -> hash1, hash5 -> hash1, hash6 -> hash1))
        }
        it("should process downloaded dependency with no more missing deps") {
          val (s1, Noop) = s.transactionReceived(tx4, Nil)
          s1 shouldBe TransactionDownloadState(
            Map(hash1 -> tx1, hash2 -> tx2, hash4 -> tx4),
            Map(hash5 -> hash1, hash3 -> hash1))
        }
      }

      describe("if only one item is missing with a lot of dependencies") {
        val s = TransactionDownloadState(
          Map(hash1 -> tx1, hash2 -> tx2, hash3 -> tx3, hash4 -> tx4, hash6 -> tx6),
          Map(hash5 -> hash1))

        it("should break up the hierarchy if the common") {
          val (s1, Noop) = s.transactionStored(hash1)
          s1 shouldBe TransactionDownloadState.empty
        }

        it("should return all dependencies to store if the missing one is received") {
          val (s1, StoreTransactions(txs)) = s.transactionReceived(tx5, Nil)
          txs should have size 6
          txs.take(3) should contain theSameElementsAs List(tx6, tx5, tx4)
          txs.slice(3, 5) should contain theSameElementsAs List(tx3, tx2)
          txs.last shouldBe tx1

          s1 shouldBe TransactionDownloadState.empty
        }
        it("should return all dependencies to store if the missing one is stored elsewhere") {
          val (s1, StoreTransactions(txs)) = s.transactionStored(hash5)
          txs should have size 5
          txs.take(2) should contain theSameElementsAs List(tx6, tx4)
          txs.slice(2, 4) should contain theSameElementsAs List(tx3, tx2)
          txs.last shouldBe tx1
          s1 shouldBe TransactionDownloadState.empty
        }
        it("should ignore already processed transactions") {
          val (s1, Noop) = s.transactionReceived(tx1, hash2 :: hash3 :: Nil)
          s1 shouldBe s
        }
        it("should ignore unknown transaction stored elsewhere") {
          val (s1, Noop) = s.transactionStored(hashx)
          s1 shouldBe s
        }
      }
    }
    describe("if double spend received") {
      /*
                  1
              /---+---\
              2       3
          /---+---\/--+---\
          4       5       x
       */

      val tx1 = TxStub(hash1, hash2 :: hash3 :: Nil)
      val tx2 = TxStub(hash2, hash4 :: hash5 :: Nil)
      val tx3 = TxStub(hash3, hash5 :: hashx :: Nil)
      val tx4 = TxStub(hash4, Nil)
      //val tx5 = TxStub(hash5, Nil)

      val s = TransactionDownloadState(
        Map(hash1 -> tx1, hash2 -> tx2),
        Map(hash4 -> hash1, hash5 -> hash1, hash3 -> hash1))

      it("should drop everything related to the doublespend detected") {
        val (s1, DoubleSpend(dropped)) = s.transactionReceived(tx3, hash5 :: hashx :: Nil)
        s1 shouldBe TransactionDownloadState.empty
        dropped should contain theSameElementsAs List(tx1, tx2)
      }
      it("should drop everything related to the doublespend detected 2") {
        val (s1, _) = s.transactionReceived(tx4, Nil)
        val (s2, DoubleSpend(dropped)) = s1.transactionReceived(tx3, hash5 :: hashx :: Nil)
        s2 shouldBe TransactionDownloadState.empty
        dropped should contain theSameElementsAs List(tx1, tx2, tx4)
      }
    }

    describe("if double spend received with different roots") {
      /*
            1       4
         /--+--\    |
        2      3    5
            /--+--\/
           x      6
       */
      val tx1 = TxStub(hash1, hash2 :: hash3 :: Nil)
      val tx2 = TxStub(hash2, Nil)
      val tx3 = TxStub(hash3, hashx :: hash6 :: Nil)
      val tx4 = TxStub(hash4, hash5 :: Nil)
      val tx5 = TxStub(hash5, hash6 :: Nil)
      val tx6 = TxStub(hash6, Nil)
      val s = TransactionDownloadState(
        Map(hash1 -> tx1, hash2 -> tx2, hash3 -> tx3, hash4 -> tx4),
        Map(hash6 -> hash1, hash5 -> hash4, hashx -> hash1))

      it("should drop the double-spending chain") {
        val (s1, DoubleSpend(rejected)) = s.transactionReceived(tx5, hash6 :: Nil)
        s1 shouldBe TransactionDownloadState(
          Map(hash1 -> tx1, hash2 -> tx2, hash3 -> tx3),
          Map(hash6 -> hash1, hashx -> hash1))
        rejected should contain theSameElementsAs List(tx4)
      }
      it("should drop everything related to the double spending transaction") {
        val (s1, Noop) = s.transactionReceived(tx6, Nil)
        s1 shouldBe TransactionDownloadState(
          Map(hash1 -> tx1, hash2 -> tx2, hash3 -> tx3, hash4 -> tx4, hash6 -> tx6),
          Map(hash5 -> hash4, hashx -> hash1))

        val (s2, DoubleSpend(rejected)) = s1.transactionReceived(tx5, hash6 :: Nil)
        rejected should contain theSameElementsAs List(tx4)
        s2 shouldBe TransactionDownloadState(
          Map(hash1 -> tx1, hash2 -> tx2, hash3 -> tx3, hash6 -> tx6),
          Map(hashx -> hash1))
      }
    }
  }
}
