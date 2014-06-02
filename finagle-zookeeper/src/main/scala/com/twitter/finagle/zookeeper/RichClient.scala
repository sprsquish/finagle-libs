package com.twitter.finagle.zookeeper

import com.twitter.conversions.time._
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.zookeeper.protocol.{
  GetDataResponse => GetDataResponsePacket,
  GetACLResponse => GetACLResponsePacket,
  ExistsResponse => ExistsResponsePacket,
  _}
import com.twitter.io.Buf
import com.twitter.util._
import java.util.concurrent.atomic.AtomicBoolean

sealed trait State
object State {
  object Connecting extends State
  object Associating extends State
  object Connected extends State
  object ConnectedReadOnly extends State
  object Closed extends State
  object AuthFailed extends State
  object NotConnected extends State
}

sealed abstract class CreateMode(val flag: Int, val ephemeral: Boolean, val sequential: Boolean)
object CreateMode {
  object Persistent extends CreateMode(0, false, false)
  object PersistentSequential extends CreateMode(2, false, true)
  object Ephemeral extends CreateMode(1, true, false)
  object EphermalSequential extends CreateMode(3, true, true)
}

case class GetACLResponse(stat: Stat, acl: Seq[ACL])
case class GetChildrenResponse(stat: Stat, children: Seq[String], watch: Option[Future[WatchedEvent]])
case class GetDataResponse(stat: Stat, data: Buf, watch: Option[Future[WatchedEvent]])

sealed trait ExistsResponse { val watch: Option[Future[WatchedEvent]] }
object ExistsResponse {
  case class NodeStat(stat: Stat, watch: Option[Future[WatchedEvent]]) extends ExistsResponse
  case class NoNode(watch: Option[Future[WatchedEvent]]) extends ExistsResponse
}


class ZkClient(
  factory: ServiceFactory[ZkRequest, ZkResponse],
  timeout: Duration = 30.seconds,
  readOnly: Boolean = false,
  watchManager: WatchManager = new DefaultWatchManager
) {
  @volatile private[this] var lastZxid: Long = 0L
  @volatile private[this] var sessionId: Long = 0L
  @volatile private[this] var sessionPasswd: Array[Byte] = new Array[Byte](16)

  // TODO: proper connection management
  private[this] def connReq = ConnectRequest(0, lastZxid, timeout.inMilliseconds.toInt, sessionId, sessionPasswd)
  private[this] def start = StartDispatcher(watchManager, readOnly, connReq)

  @volatile private[this] var dispatcher: Future[Service[ZkRequest, ZkResponse]] = newDispatcher flatMap { d =>
    // XXX: a) figure out why we can't just call close(); b) figure out how to handle conn initialization better
    // it seems that starting with 0s makes the ZK server sad
    d(CloseConn(Time.now)) flatMap { _ => newDispatcher }
  }

  private[this] def newDispatcher: Future[Service[ZkRequest, ZkResponse]] =
    factory() flatMap { svc =>
      svc(start) flatMap {
        case PacketResponse(_, rep: ConnectResponse) =>
          sessionId = rep.sessionId
          sessionPasswd = rep.passwd
          // TODO: set negotiated timeout
          dispatcher = Future.value(svc)
          dispatcher
        case _ =>
          Future.exception(new Exception("should have gotten a ConnectResponse"))
      }
    }

  private[this] val connected = new AtomicBoolean(true)
  private[this] def reconnect(): Future[Service[ZkRequest, ZkResponse]] = {
    dispatcher = newDispatcher
    dispatcher onSuccess { _ => connected.set(true) }
    dispatcher
  }

  private[this] def write[T <: Packet](req: ZkRequest): Future[T] =
    dispatcher flatMap { svc =>
      svc(req) flatMap {
        // XXX: shouldn't need this
        case ClosedConn => throw new Exception("should not have hit this")
        case ErrorResponse(zxid, err) =>
          lastZxid = zxid
          Future.exception(err)
        case PacketResponse(zxid, packet) =>
          lastZxid = zxid
          Future.value(packet.asInstanceOf[T])
      }
    } rescue {
      case KeeperException.ConnectionLoss =>
        if (connected.compareAndSet(true, false))
          reconnect() flatMap { _ => write(req) }
        else
          write(req)
    }

  def create(
    path: String,
    data: Buf = Buf.Empty,
    acl: Seq[ACL] = Ids.OpenAclUnsafe,
    createMode: CreateMode = CreateMode.Persistent
  ): Future[String] = validatePath(path) flatMap { _ =>
    val bytes = new Array[Byte](data.length)
    data.write(bytes, 0)
    val req = PacketRequest(OpCodes.Create, CreateRequest(path, bytes, acl, createMode.flag), CreateResponse.unapply)
    write[CreateResponse](req) map { _.path }
  }

  def delete(path: String, version: Int): Future[Unit] = validatePath(path) flatMap { _ =>
    write[DeleteResponse](PacketRequest(OpCodes.Delete, DeleteRequest(path, version), DeleteResponse.unapply)).unit
  }

  def exists(path: String, watch: Boolean = false): Future[ExistsResponse] = validatePath(path) flatMap { _ =>
    val watchFuture = if (watch) Some(watchManager.existsWatch(path)) else None

    val req = PacketRequest(OpCodes.Exists, ExistsRequest(path, watch), ExistsResponsePacket.unapply)
    write[ExistsResponsePacket](req) transform {
      case Return(ExistsResponsePacket(stat)) => Future.value(ExistsResponse.NodeStat(stat, watchFuture))
      case Throw(KeeperException.NoNode) => Future.value(ExistsResponse.NoNode(watchFuture))
    }
  }

  def getACL(path: String): Future[GetACLResponse] = validatePath(path) flatMap { _ =>
    val req = PacketRequest(OpCodes.GetACL, GetACLRequest(path), GetACLResponsePacket.unapply)
    write[GetACLResponsePacket](req) map { rep => GetACLResponse(rep.stat, rep.acl) }
  }

  def getChildren(path: String, watch: Boolean = false): Future[GetChildrenResponse] = validatePath(path) flatMap { _ =>
    val watchFuture = if (watch) Some(watchManager.childrenWatch(path)) else None

    val req = PacketRequest(OpCodes.GetChildren, GetChildren2Request(path, watch), GetChildren2Response.unapply)
    write[GetChildren2Response](req) map { rep => GetChildrenResponse(rep.stat, rep.children, watchFuture) }
  }

  def getData(path: String, watch: Boolean = false): Future[GetDataResponse] = validatePath(path) flatMap { _ =>
    val watchFuture = if (watch) Some(watchManager.dataWatch(path)) else None

    val req = PacketRequest(OpCodes.GetData, GetDataRequest(path, watch), GetDataResponsePacket.unapply)
    write[GetDataResponsePacket](req) map { rep => GetDataResponse(rep.stat, Buf.ByteArray(rep.data), watchFuture) }
  }

  def setACL(path: String, acl: Seq[ACL], version: Int): Future[Stat] = validatePath(path) flatMap { _ =>
    val req = PacketRequest(OpCodes.SetACL, SetACLRequest(path, acl, version), SetACLResponse.unapply)
    write[SetACLResponse](req) map { _.stat }
  }

  def setData(path: String, data: Buf, version: Int): Future[Stat] = validatePath(path) flatMap { _ =>
    val bytes = new Array[Byte](data.length)
    data.write(bytes, 0)
    val req = PacketRequest(OpCodes.SetData, SetDataRequest(path, bytes, version), SetDataResponse.unapply)
    write[SetDataResponse](req) map { _.stat }
  }

  //def multi(ops: Seq[Op]): Future[Seq[OpResult]]

  private[zookeeper] def validatePath(path: String): Future[Unit] = {
    if (path == null)
      return Future.exception(new IllegalArgumentException("Path cannot be null"))

    if (path.length() == 0)
      return Future.exception(new IllegalArgumentException("Path length must be > 0"))

    if (path.charAt(0) != '/')
      return Future.exception(new IllegalArgumentException("Path must start with / character"))

    if (path.length == 1)
      return Future.Done

    if (path.charAt(path.length() - 1) == '/')
      return Future.exception(new IllegalArgumentException("Path must not end with / character"))

    def err(reason: String): Future[Unit] =
      Future.exception(new IllegalArgumentException("Invalid path string \"" + path + "\" caused by " + reason))

    path.split("/").drop(1) foreach {
      case "" => return err("empty node name specified")
      case ".." | "." => return err("relative paths not allowed")
      case s => s foreach { c =>
        if (c == 0) return err("null character not allowed")

        if (c > '\u0000' && c <= '\u001f'
          || c >= '\u007f' && c <= '\u009F'
          || c >= '\ud800' && c <= '\uf8ff'
          || c >= '\ufff0' && c <= '\uffff'
        ) return err("invalid character")
      }
    }

    Future.Done
  }
}
