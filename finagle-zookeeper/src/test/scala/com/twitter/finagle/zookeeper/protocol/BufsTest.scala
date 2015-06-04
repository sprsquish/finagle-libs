package com.twitter.finagle.zookeeper.protocol

import com.twitter.io.Buf
import java.util.Arrays
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BufsTest extends FunSuite {
  test("string") {
    val Buf.U32BE(6, Buf.Utf8("string")) = BufString("string")

    val BufString("string", Buf.Empty) = BufString("string")

    val extraBuf = Buf.U32BE(1234)
    val BufString("string", rem) = BufString("string").concat(extraBuf)
    assert(rem === extraBuf)
  }

  test("array") {
    val bytes = Array[Byte](1,2,3,4)

    val Buf.U32BE(4, Buf.ByteArray(bytesOut, _, _)) = BufArray(bytes)
    assert(Arrays.equals(bytes, bytesOut))

    val BufArray(bytesOut2, Buf.Empty) = BufArray(bytes)
    assert(Arrays.equals(bytes, bytesOut2))

    val extraBuf = Buf.U32BE(1234)
    val BufArray(bytesOut3, rem) = BufArray(bytes).concat(extraBuf)
    assert(Arrays.equals(bytes, bytesOut3))
    assert(rem === extraBuf)
  }

  test("bool") {
    val Buf.ByteArray(out, _, _) = BufBool(true)
    assert(out(0) === 1)

    val Buf.ByteArray(out2, _, _) = BufBool(false)
    assert(out2(0) === 0)

    val BufBool(true, Buf.Empty) = BufBool(true)
    val BufBool(false, Buf.Empty) = BufBool(false)

    val extraBuf = Buf.U32BE(1234)
    val BufBool(bytesOut3, rem) = BufBool(true).concat(extraBuf)
    assert(rem === extraBuf)
  }

  test("seq") {
    val Buf.U32BE(3, Buf.U64LE(1L, Buf.U64LE(2L, Buf.U64LE(3L, Buf.Empty)))) =
      BufSeq(Seq(1L,2L,3L), Buf.U64LE.apply)

    val seq = Seq(1,2,3)
    val BufSeq(out1, Buf.Empty) = (BufSeq(seq, Buf.U32BE.apply), Buf.U32BE.unapply(_))
    assert(seq === out1)

    val extraBuf = Buf.U32BE(1234)
    val BufSeq(out2, rem) = (BufSeq(seq, Buf.U32BE.apply).concat(extraBuf), Buf.U32BE.unapply(_))
    assert(rem === extraBuf)
    assert(seq === out2)
  }

  test("seq string") {
    val BufSeqString(Seq("1","2"), Buf.Empty) = BufSeqString(Seq("1","2"))
  }

  test("seq ACL") {
    val BufSeqACL(Seq(ACL(1, Id("foo", "bar")), ACL(3, Id("baz", "buz"))), Buf.Empty) =
      BufSeqACL(Seq(ACL(1, Id("foo", "bar")), ACL(3, Id("baz", "buz"))))
  }

  test("seq id") {
    val BufSeqId(Seq(Id("foo", "bar"), Id("baz", "buz")), Buf.Empty) =
      BufSeqId(Seq(Id("foo", "bar"), Id("baz", "buz")))
  }

  test("seq txn") {
    val BufSeqTxn(Seq(Txn(1, _), Txn(2, _)), Buf.Empty) =
      BufSeqTxn(Seq(Txn(1, Array.empty[Byte]), Txn(2, Array.empty[Byte])))
  }
}
