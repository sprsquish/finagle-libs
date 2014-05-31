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

sealed trait CreateMode
//TODO

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
  watchManager: WatchManager = DefaultWatchManager,
  timeout: Duration = 30.seconds,
  readOnly: Boolean = false
) {
  @volatile private[this] var lastZxid: Long = 0L
  @volatile private[this] var sessionId: Long = 0L
  @volatile private[this] var sessionPasswd: Array[Byte] = new Array[Byte](16)

  // TODO: proper connection management
  private[this] def connReq = ConnectRequest(0, lastZxid, timeout.inMilliseconds.toInt, sessionId, sessionPasswd)
  private[this] val start = StartDispatcher(watchManager, timeout.inMilliseconds.toInt, readOnly, connReq)
  private[this] val svc: Future[Service[ZkRequest, ZkResponse]] = factory() flatMap { svc =>
    svc(start) flatMap {
      case PacketResponse(_, rep: ConnectResponse) =>
        sessionId = rep.sessionId
        sessionPasswd = rep.passwd
        // TODO: set negotiated timeout
        Future.value(svc)
      case _ =>
        Future.exception(new Exception("should have gotten a ConnectResponse"))
    }
  }

  private[this] def write[T <: Packet](req: ZkRequest): Future[T] = svc flatMap { svc =>
    svc(req) flatMap {
      case ErrorResponse(zxid, err) =>
        lastZxid = zxid
        Future.exception(err)
      case PacketResponse(zxid, packet) =>
        lastZxid = zxid
        Future.value(packet.asInstanceOf[T])
    }
  }

  def create(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode): Future[String] = {
    val req = PacketRequest(OpCodes.Create, CreateRequest(path, data, acl, 0), CreateResponse.unapply)
    write[CreateResponse](req) map { _.path }
  }

  def delete(path: String, version: Int): Future[Unit] = {
    write[DeleteResponse](PacketRequest(OpCodes.Delete, DeleteRequest(path, version), DeleteResponse.unapply)).unit
  }

  def exists(path: String, watch: Boolean = false): Future[ExistsResponse] = {
    val watchFuture = if (watch) Some(watchManager.existsWatch(path)) else None

    val req = PacketRequest(OpCodes.Exists, ExistsRequest(path, watch), ExistsResponsePacket.unapply)
    write[ExistsResponsePacket](req) transform {
      case Return(ExistsResponsePacket(stat)) => Future.value(ExistsResponse.NodeStat(stat, watchFuture))
      case Throw(KeeperException.NoNode) => Future.value(ExistsResponse.NoNode(watchFuture))
    }
  }

  def getACL(path: String): Future[GetACLResponse] = {
    val req = PacketRequest(OpCodes.GetACL, GetACLRequest(path), GetACLResponsePacket.unapply)
    write[GetACLResponsePacket](req) map { rep => GetACLResponse(rep.stat, rep.acl) }
  }

  def getChildren(path: String, watch: Boolean = false): Future[GetChildrenResponse] = {
    val watchFuture = if (watch) Some(watchManager.childrenWatch(path)) else None

    val req = PacketRequest(OpCodes.GetChildren, GetChildren2Request(path, watch), GetChildren2Response.unapply)
    write[GetChildren2Response](req) map { rep => GetChildrenResponse(rep.stat, rep.children, watchFuture) }
  }

  def getData(path: String, watch: Boolean = false): Future[GetDataResponse] = {
    val watchFuture = if (watch) Some(watchManager.dataWatch(path)) else None

    val req = PacketRequest(OpCodes.GetData, GetDataRequest(path, watch), GetDataResponsePacket.unapply)
    write[GetDataResponsePacket](req) map { rep => GetDataResponse(rep.stat, BufArray(rep.data), watchFuture) }
  }

  def setACL(path: String, acl: Seq[ACL], version: Int): Future[Stat] = {
    val req = PacketRequest(OpCodes.SetACL, SetACLRequest(path, acl, version), SetACLResponse.unapply)
    write[SetACLResponse](req) map { _.stat }
  }

  def setData(path: String, data: Buf, version: Int): Future[Stat] = {
    val bytes = new Array[Byte](data.length)
    data.write(bytes, 0)
    val req = PacketRequest(OpCodes.SetData, SetDataRequest(path, bytes, version), SetDataResponse.unapply)
    write[SetDataResponse](req) map { _.stat }
  }

  //def multi(ops: Seq[Op]): Future[Seq[OpResult]]
}
