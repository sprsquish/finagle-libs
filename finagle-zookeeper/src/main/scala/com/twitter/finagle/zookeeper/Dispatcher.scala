package com.twitter.finagle.zookeeper

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.transport.{Transport => FTransport}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.zookeeper.protocol._
import com.twitter.io.Buf
import com.twitter.util._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.LinkedBlockingQueue

class ConnectionNotStarted extends Exception
class ConnectionAlreadyStarted extends Exception
class EmptyRequestQueueException(xid: Int) extends Exception
class OutOfOrderException extends Exception

sealed trait ConnectionState
object ConnectionState {
  object Connecting extends ConnectionState
  object Associating extends ConnectionState
  object Connected extends ConnectionState
  object ConnectedReadOnly extends ConnectionState
  object Closed extends ConnectionState
  object AuthFailed extends ConnectionState
  object NotConnected extends ConnectionState
}

sealed trait ZkRequest
case class StartDispatcher(
  watchManager: WatchManager,
  readOnly: Boolean,
  connPacket: ConnectRequest
) extends ZkRequest

case class PacketRequest[Response <: Packet](
  packet: RequestPacket[Response]
) extends ZkRequest

sealed trait ZkResponse { val zxid: Long }
case class PacketResponse(zxid: Long, packet: Packet) extends ZkResponse
case class ErrorResponse(zxid: Long, err: KeeperException) extends ZkResponse

private[finagle] class ClientDispatcher(
  trans: FTransport[Buf, Buf],
  ttimer: Timer = DefaultTimer.twitter
) extends Service[ZkRequest, ZkResponse] {
  implicit private[this] val timer = ttimer

  private[this] val curXid = new AtomicInteger(0)
  private[this] val started = new AtomicBoolean(false)
  private[this] val queue = new LinkedBlockingQueue[PendingResponse]()

  private class PendingResponse(decoder: Buf => Option[(Packet, Buf)]) {
    val rep = new Promise[ZkResponse]
    val xid = curXid.incrementAndGet()

    def apply(header: ReplyHeader, buf: Buf) {
      val err = KeeperException(header.err)
      if (err != KeeperException.Ok) rep.setValue(ErrorResponse(header.zxid, err)) else {
        decoder(buf) match {
          case Some((packet, _)) => rep.setValue(PacketResponse(header.zxid, packet))
          case None => // TODO: invalid response
        }
      }
    }

    def apply(exp: Throwable) {
      rep.setException(exp)
    }
  }

  private[this] def actOnRead(watchManager: WatchManager)(buf: Buf): Future[Unit] = {
    val ReplyHeader(header, rem) = buf
    header match {
      // ping
      case ReplyHeader(XID.Ping, _, _) =>
        // TODO: debug logging?
        Future.Done

      // auth packet
      case ReplyHeader(XID.Auth, _, err) =>
        if (err != KeeperException.AuthFailed) Future.Done else {
          // TODO: state = State.AuthFailed
          watchManager(WatchedEvent(EventType.None, KeeperState.AuthFailed))
        }

      // notification
      case ReplyHeader(XID.Notification, _, _) =>
        val WatcherEvent(evt, _) = rem
        watchManager(evt)

      // TODO: implement sasl auth
      // sasl auth in progress
      //case _ if saslAuth =>
        //val GetSASLRequest(saslReq, _) = rem
        //Future.Done

      case ReplyHeader(xid, zxid, _) =>
        val rep = queue.synchronized { queue.poll() }

        // we don't have any requests to service. this is an unrecoverable exception
        if (rep == null) throw new EmptyRequestQueueException(xid)

        // server and client are somehow out of sync. we can't recover
        if (rep.xid != xid) throw new OutOfOrderException

        rep(header, rem)
        Future.Done
    }
  }

  // TODO: calculate how often?
  private[this] val pingBuf = RequestHeader(XID.Ping, OpCodes.Ping).buf
  private[this] def sendPingLooper(delay: Duration): Future[Unit] =
    trans.write(pingBuf) delayed(delay) before sendPingLooper(delay)

  private[this] def readLooper(timeout: Duration, watchManager: WatchManager): Future[Unit] =
    trans.read() within(timeout) flatMap actOnRead(watchManager) before readLooper(timeout, watchManager)

  private[this] def cleanup(exp: Throwable): Unit = close() ensure {
    queue.synchronized {
      var item = queue.poll()
      while (item != null) {
        item(exp)
        item = queue.poll()
      }
    }
  }

  private[this] def start(req: StartDispatcher): Future[ConnectResponse] = {
    val StartDispatcher(watchManager, readOnly, connPacket) = req
    trans.write(connPacket.buf.concat(BufBool(readOnly))) flatMap { _ =>
      trans.read() map { case ConnectResponse(rep, _) =>
        readLooper(rep.timeOut.milliseconds, watchManager) onFailure {
          case _: TimeoutException => cleanup(KeeperException.ConnectionLoss)
          case t: Throwable => cleanup(t)
        }
        sendPingLooper(10.seconds) // max time between pings
        rep
      }
    }
  }

  def apply(req: ZkRequest): Future[ZkResponse] = req match {
    case sd: StartDispatcher =>
      if (started.getAndSet(true))
        Future.exception(new ConnectionAlreadyStarted)
      else
        start(sd) map { rep => PacketResponse(0, rep) }

    // sync to ensure packets go into the queue and transport at the same time
    case PacketRequest(packet) if started.get() => synchronized {
      val rep = new PendingResponse(packet.decodeResponse)
      queue.add(rep)
      trans.write(RequestHeader(rep.xid, packet.opCode).buf.concat(packet.buf)) flatMap { _ => rep.rep }
    }

    case _ if !started.get() =>
      Future.exception(new ConnectionNotStarted)
  }

  override def close(deadline: Time): Future[Unit] = {
    trans.close(deadline) onSuccess { _ => started.set(false) }
  }
}
