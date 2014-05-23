package com.twitter.finagle.zookeeper

private[finagle] class ClientDispatcher(
  watchManager: WatchManager,
  trans: Transport[Buf, Buf]
) extends Service[Packet, Packet] {
  case class Request(xid: Int, reqPacket: Packet)

  private[this] queue = new LinkedBlockingQueue[(Request, Promise[Packet])]()

  private[this] def actOnRead(buf: Buf): Future[Unit] = {
    val ReplyHeader(replyHeader, rem) = buf
    replyHeader match {
      // ping
      case -2 => // TODO: debug logging?

      // auth packet
      case -4 =>

      // notification
      case -1 =>
        val WatcherEvent(evt, _) = rem
        watchManager.processEvent(evt)

      // sasl auth in progress
      case _ if saslAuth =>

      // nothing left to check via match
      case id =>
        val req = queue.poll()

        // we don't have any requests to service. this is an unrecoverable exception
        if (req == null) throw new EmptyRequestQueueException(id)

        val (request, promise) = req

        // server and client are somehow out of sync. we can't recover
        if (request.xid != id) throw new OutOfOrderException(request, replyHeader)

        promise.update(deserialize(request, replyHeader, rem))
    }
    Future.Done
  }

  private[this] def deserialize(req: Request, repHeader: ReplyHeader, buf: Buf): Try[Response] = {
  }

  private[this] def readLoop(): Future[Unit] =
    trans.read() flatMap actOnRead before readLoop()
  readLoop()

  def apply(req: Request): Future[Response] = req match {
  }

  override def close(deadline: Time): Future[Unit] =
    trans.close(deadline)
}
