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

class ConnectionAlreadyStarted extends Exception
class EmptyRequestQueueException(xid: Int) extends Exception
class OutOfOrderException extends Exception

sealed trait ZkRequest
case class StartDispatcher(
  watchManager: WatchManager,
  timeout: Int,
  readOnly: Boolean,
  connPacket: ConnectRequest
) extends ZkRequest

case class PacketRequest(
  opCode: Int,
  packet: Packet,
  decoder: Buf => Option[(Packet, Buf)]
) extends ZkRequest

sealed trait ZkResponse { val zxid: Long }
case class PacketResponse(
  zxid: Long,
  packet: Packet
) extends ZkResponse

case class ErrorResponse(
  zxid: Long,
  err: KeeperException
) extends ZkResponse

private[finagle] class ClientDispatcher(
  trans: FTransport[Buf, Buf],
  ttimer: Timer = DefaultTimer.twitter
) extends Service[ZkRequest, ZkResponse] {
  implicit private[this] val timer = ttimer
  private[this] val curXid = new AtomicInteger(0)
  private[this] val started = new AtomicBoolean(false)
  private[this] val queue = new LinkedBlockingQueue[(Int, Buf => Option[(Packet, Buf)], Promise[ZkResponse])]()

  private[this] def actOnRead(watchManager: WatchManager)(buf: Buf): Future[Unit] = {
    val ReplyHeader(replyHeader, rem) = buf
    println("<== " + replyHeader)
    replyHeader match {
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

      case ReplyHeader(xid, zxid, KeeperException(err)) =>
        val req = synchronized { queue.poll() }

        // we don't have any requests to service. this is an unrecoverable exception
        if (req == null) throw new EmptyRequestQueueException(xid)

        val (reqXid, decoder, promise) = req

        // server and client are somehow out of sync. we can't recover
        if (reqXid != xid) throw new OutOfOrderException//(request, replyHeader)

        if (err != KeeperException.Ok) promise.setValue(ErrorResponse(zxid, err)) else {
          decoder(rem) match {
            case Some((packet, _)) => promise.setValue(PacketResponse(zxid, packet))
            case None => // TODO: invalid response
          }
        }
        Future.Done
    }
  }

  // TODO: calculate how often?
  private[this] val pingBuf = RequestHeader(XID.Ping, OpCodes.Ping).buf
  private[this] def sendPingLooper(delay: Duration): Future[Unit] =
    trans.write(pingBuf) delayed(delay) before sendPingLooper(delay)

  private[this] def readLooper(timeout: Duration, watchManager: WatchManager): Future[Unit] =
    trans.read() within(timeout) flatMap actOnRead(watchManager) before readLooper(timeout, watchManager)

  private[this] def cleanup(exp: Throwable): Unit = synchronized {
    var item = queue.poll()
    while (item != null) {
      val (_, _, p) = item
      p.setException(exp)
      item = queue.poll()
    }
    close()
  }

  def apply(req: ZkRequest): Future[ZkResponse] = {
    println("==> " + req)
    req match {
    case StartDispatcher(watchManager, timeout, readOnly, connPacket) =>
      if (started.getAndSet(true)) Future.exception(new ConnectionAlreadyStarted) else {

        trans.write(connPacket.buf.concat(BufBool(readOnly))) flatMap { _ =>
          trans.read() map { case ConnectResponse(rep, _) =>
println("con <=: " + rep)
          PacketResponse(0, rep) }
        } onSuccess { _ =>
          readLooper(timeout.seconds, watchManager) onFailure cleanup
          sendPingLooper(10.seconds) // max time between pings
        }
      }

    case PacketRequest(opCode, packet, decoder) =>
      val repPromise = new Promise[ZkResponse]
      val xId = curXid.incrementAndGet()
      val reqBuf = RequestHeader(xId, opCode).buf.concat(packet.buf)
      // sync to ensure packets go into the queue and transport at the same time
      synchronized {
        queue.add((xId, decoder, repPromise))
        trans.write(reqBuf) flatMap { _ => repPromise }
      }
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    trans.close(deadline)
  }
}
