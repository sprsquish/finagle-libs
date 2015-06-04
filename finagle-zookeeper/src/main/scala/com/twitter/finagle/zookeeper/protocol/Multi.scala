package com.twitter.finagle.zookeeper.protocol

import com.twitter.io.Buf
import scala.annotation.tailrec

object Multi {
  sealed abstract class Op(opCode: Int) {
    def body: Packet
    //XXX: there must be a better way to handle the return type
    def handlePathIn(fixPath: String => String): Op
    def buf: Buf = MultiHeader(opCode, false, -1).buf.concat(body.buf)
  }

  object Op {
    case class Create(path: String, data: Buf, acl: Seq[ACL], createMode: CreateMode) extends Op(OpCodes.Create2) {
      def handlePathIn(fixPath: String => String) = copy(path = fixPath(path))
      def body: Packet = CreateRequest(path, BufArray.toBytes(data), acl, createMode.flag)
    }

    case class Delete(path: String, version: Int) extends Op(OpCodes.Delete) {
      def handlePathIn(fixPath: String => String): Delete = copy(path = fixPath(path))
      def body: Packet = DeleteRequest(path, version)
    }

    case class SetData(path: String, data: Buf, version: Int) extends Op(OpCodes.SetData) {
      def handlePathIn(fixPath: String => String): SetData = copy(path = fixPath(path))
      def body: Packet = SetDataRequest(path, BufArray.toBytes(data), version)
    }

    case class Check(path: String, version: Int) extends Op(OpCodes.Check) {
      def handlePathIn(fixPath: String => String): Check = copy(path = fixPath(path))
      def body: Packet = CheckVersionRequest(path, version)
    }
  }

  sealed trait OpResult {
    //XXX: there must be a better way to handle the return type
    def handlePathOut(fixPath: String => String): OpResult = this
  }

  object OpResult {
    case class Create(path: String, stat: Stat) extends OpResult {
      override def handlePathOut(fixPath: String => String) = copy(path = fixPath(path))
    }

    object Delete extends OpResult

    case class SetData(stat: Stat) extends OpResult

    object Check extends OpResult

    case class Error(err: Int) extends OpResult
  }

  def encode(ops: Seq[Op]): Buf =
    ops.foldLeft(Buf.Empty)((buf, op) => buf.concat(op.buf))
      .concat(MultiHeader(-1, true, -1).buf)

  def decode(buf: Buf): (Seq[OpResult], Buf) =
    decode(buf, Seq.empty[OpResult])

  @tailrec
  private[this] def decode(buf: Buf, results: Seq[OpResult]): (Seq[OpResult], Buf) = {
    val MultiHeader(header, opBuf) = buf
    if (header.done) (results, opBuf) else {
      val (res, rem) = header.typ match {
        case OpCodes.Create2 =>
          val Create2Response(rep, rem) = opBuf
          (OpResult.Create(rep.path, rep.stat), rem)

        case OpCodes.Delete =>
          val DeleteResponse(rep, rem) = opBuf
          (OpResult.Delete, rem)

        case OpCodes.SetData =>
          val SetDataResponse(rep, rem) = opBuf
          (OpResult.SetData(rep.stat), rem)

        case OpCodes.Check =>
          (OpResult.Check, opBuf)

        case OpCodes.Error =>
          val ErrorResponse(rep, rem) = opBuf
          (OpResult.Error(rep.err), rem)

        case typ =>
          throw new Exception("Invalid type %d in MultiResponse".format(typ))
      }
      decode(rem, results :+ res)
    }
  }
}
