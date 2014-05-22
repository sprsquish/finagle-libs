package com.twitter.finagle.zookeeper

case class Transport(
  trans: FTransport[ChannelBuffer, ChannelBuffer]
) extends FTransport[Buf, Buf] {
  def isOpen = trans.isOpen
  val onClose = trans.onClose
  def localAddress = trans.localAddress
  def remoteAddress = trans.remoteAddress
  def close(deadline: Time) = trans.close(deadline)

  def write(req: Buf): Future[Unit] =
    trans.write(BufChannelBuffer(BufInt(req.length).concat(req)))

  def read(): Future[Buf] =
    read(4) flatMap { case BufInt(len) => read(len) }

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
