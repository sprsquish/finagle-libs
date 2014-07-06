package com.twitter.finagle.zookeeper

import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.finagle.transport.{Transport => FTransport}
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

case class Transport(
  trans: FTransport[ChannelBuffer, ChannelBuffer]
) extends FTransport[Buf, Buf] {
  def isOpen = trans.isOpen
  val onClose = trans.onClose
  def localAddress = trans.localAddress
  def remoteAddress = trans.remoteAddress
  def close(deadline: Time) = trans.close(deadline)

  def write(req: Buf): Future[Unit] = {
    val framedReq = Buf.U32BE(req.length).concat(req)
    trans.write(BufChannelBuffer(framedReq))
  }

  // the dispatcher runs a single read loop. this is safe
  def read(): Future[Buf] =
    read(4) flatMap { case Buf.U32BE(len, Buf.Empty) => read(len) }

  @volatile private[this] var buf = Buf.Empty
  private[this] def read(len: Int): Future[Buf] =
    if (buf.length < len) {
      trans.read flatMap { chanBuf =>
        buf = buf.concat(ChannelBufferBuf(chanBuf))
        read(len)
      }
    } else {
      val out = buf.slice(0, len)
      buf = buf.slice(len, buf.length)
      Future.value(out)
    }
}
