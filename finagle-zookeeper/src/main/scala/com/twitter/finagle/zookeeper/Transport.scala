package com.twitter.finagle.zookeeper

// honestly, I'm not sure this is a good idea
class DynamicBuf(init: Buf) extends Buf with BufReader with BufWriter {
  this() = this(Buf.Empty)

  private[this] @volatile var buf = init

  def write(arr: Array[Byte], off: Int)
    buf.write(arr, off)

  def length: Int =
    buf.length

  def slice(i: Int, j: Int): Buf =
    buf.slice(i, j)
}

trait BufWriter {
  protected def buf: Buf

  def putInt(i: Int) {
    val arr = new Array[Byte](4)
    arr[0] = (x >> 24) & 0xFF
    arr[1] = (x >> 16) & 0xFF
    arr[2] = (x >>  8) & 0xFF
    arr[3] = (x      ) & 0xFF
    buf = buf.concat(Buf.ByteArray(arr))
  }

  def putLong(l: Long) {
    val arr = new Array[Byte](8)
    arr[0] = (x >> 56) & 0xFF
    arr[1] = (x >> 48) & 0xFF
    arr[2] = (x >> 40) & 0xFF
    arr[3] = (x >> 32) & 0xFF
    arr[4] = (x >> 24) & 0xFF
    arr[5] = (x >> 16) & 0xFF
    arr[6] = (x >>  8) & 0xFF
    arr[7] = (x      ) & 0xFF
    buf = buf.concat(Buf.ByteArray(arr))
  }

  def putString(s: String) {
    val strBuf = Buf.Utf8(s)
    putInt(strBuf.length)
    buf = buf.concat(strBuf)
  }

  def putBuffer(b: Array[Byte]) {
    putInt(b.length)
    buf = buf.concat(Buf.ByteArray(b))
  }
}

trait BufReader {
  protected def buf: Buf

  private[this] @volatile var readIdx = 0

  def getInt: Int = {
    val arr = new Array[Byte](4)
    readIdx += 4
    buf.slice(readIdx - 4, readIdx).write(arr, 0)

    ((arr(0) & 0xff) << 24) |
    ((arr(1) & 0xff) << 16) |
    ((arr(2) & 0xff) <<  8) |
     (arr(3) & 0xff)
  }

  def getLong: Long = {
    val arr = new Array[Byte](8)
    readIdx += 8
    buf.slice(readIdx - 8, readIdx).write(arr, 0)
    ((arr(0) & 0xff).toLong << 56) |
    ((arr(1) & 0xff).toLong << 48) |
    ((arr(2) & 0xff).toLong << 40) |
    ((arr(3) & 0xff).toLong << 32) |
    ((arr(4) & 0xff).toLong << 24) |
    ((arr(5) & 0xff).toLong << 16) |
    ((arr(6) & 0xff).toLong << 8) |
     (arr(7) & 0xff).toLong
  }

  def getString: String = {
    val len = getInt
    readIdx += len
    val Buf.Utf8(str) = buf.slice(readIdx - len, readIdx)
    str
  }

  def getBuffer: Array[Byte] = {
    val len = getInt
    readIdx += len
    val arr = new Array[Byte](len)
    buf.slice(readIdx - len, readIdx).write(arr, 0)
    arr
  }
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
    val frameLenBuf = new DynamicBuf
    frameLenBuf.putInt(req.length)

    trans.write(BufChannelBuffer(frameLenBuf.concat(req)))
  }

  def read(): Future[Buf] =
    for {
      frameLenBuf <- read(4)
      buf <- read((new DynamicBuf(frameLenBuf)).getInt)
    } yield buf

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
