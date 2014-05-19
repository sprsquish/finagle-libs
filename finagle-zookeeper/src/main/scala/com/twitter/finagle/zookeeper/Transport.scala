package com.twitter.finagle.zookeeper

object BufInt {
  def apply(i: Int): Buf = {
    val arr = new Array[Byte](4)
    arr[0] = (i >> 24) & 0xFF
    arr[1] = (i >> 16) & 0xFF
    arr[2] = (i >>  8) & 0xFF
    arr[3] = (i      ) & 0xFF
    Buf.ByteArray(arr)
  }

  def unapply(buf: Buf): Option[(Int, Buf)] = {
    val arr = new Array[Byte](4)
    readIdx += 4
    buf.slice(readIdx - 4, readIdx).write(arr, 0)

    val out =
      ((arr(0) & 0xff) << 24) |
      ((arr(1) & 0xff) << 16) |
      ((arr(2) & 0xff) <<  8) |
       (arr(3) & 0xff)
    Some((out, buf.slice(4, buf.length))
  }
}

object BufLong {
  def apply(l: Long): Buf = {
    val arr = new Array[Byte](8)
    arr[0] = (l >> 56) & 0xFF
    arr[1] = (l >> 48) & 0xFF
    arr[2] = (l >> 40) & 0xFF
    arr[3] = (l >> 32) & 0xFF
    arr[4] = (l >> 24) & 0xFF
    arr[5] = (l >> 16) & 0xFF
    arr[6] = (l >>  8) & 0xFF
    arr[7] = (l      ) & 0xFF
    Buf.ByteArray(arr)
  }

  def unapply(buf: Buf): Option[(Long, Buf)] = {
    val arr = new Array[Byte](8)
    buf.slice(0, 8).write(arr, 0)

    val out =
      ((arr(0) & 0xff) << 56) |
      ((arr(1) & 0xff) << 48) |
      ((arr(2) & 0xff) << 40) |
      ((arr(3) & 0xff) << 32) |
      ((arr(4) & 0xff) << 24) |
      ((arr(5) & 0xff) << 16) |
      ((arr(6) & 0xff) <<  8) |
       (arr(7) & 0xff)
    Some((out, buf.slice(8, buf.length))
  }
}

object BufString {
  def apply(s: String): Buf = {
    val strBuf = Buf.Utf8(s)
    BufInt(strBuf.length).concat(strBuf)
  }

  def unapply(buf: Buf): Option[(String, Buf)] = {
    val BufInt(len, rem) = buf
    val Buf.Utf8(str) = rem
    Some((str, rem.slice(len, rem.length))
  }
}

object BufArray {
  def apply(a: Array[Byte]): Buf = {
    val arrBuf = Buf.ByteArray(a)
    BufInt(arrBuf.length).concat(arrBuf)
  }

  def unapply(buf: Buf): Option[(String, Buf)] = {
    val BufInt(len, rem) = buf
    val arr = new Array[Byte](len)
    rem.write(arr, 0)
    Some((arr, rem.slice(len, rem.length))
  }
}

object BufBool {
  def apply(b: Boolean): Buf = {
    BufInt(if (b) 1 else 0)
  }

  def unapply(buf: Buf): Option[(Boolean, Buf)] = {
    val BufInt(i, rem) = buf
    // TODO: throw if i < 0
    Some((i != 0, rem))
  }
}

trait Packet {
  def serialized: Buf
}

case class ZkTransport(
  trans: Transport[ChannelBuffer, ChannelBuffer]
) extends Transport[Buf, Buf] {
  def isOpen = trans.isOpen
  val onClose = trans.onClose
  def localAddress = trans.localAddress
  def remoteAddress = trans.remoteAddress
  def close(deadline: Time) = trans.close(deadline)

  def write(req: Buf): Future[Unit] = {
    val framedBuf = BufInt(req.length).concat(req)
    trans.write(BufChannelBuffer(framedBuf))
  }

  def read(): Future[Buf] = read(4) flatMap {
    case BufInt(frameLen, _) => read(frameLen)
  }

  private[this] @volatile var buf = Buf.Empty
  private[this] def read(len: Int): Future[Buf] =
    if (buf.length < len) {
      trans.read flatMap { chanBuf =>
        buf = buf.concat(ChannelBuffer(chanBuf))
        read(len)
      }
    } else {
      val out = buf.slice(0, len)
      buf = buf.slice(len, buf.length)
      Future.value(out)
    }
}
