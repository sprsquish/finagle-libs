package com.twitter.finagle.zookeeper

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

sealed trait ExistsResponse { val watch: Option[Promise[WatchedEvent]] }
object ExistsResponse {
  case class NodeStat(stat: Stat, watch: Option[Promise[WatchedEvent]]) extends ExistsResponse
  case class NoNode(watch: Option[Promise[WatchedEvent]]) extends ExistsResponse
}

class ZkClient(
  factory: ServiceFactory[ZkRequest, Packet],
  watchManager: WatchManager = WatchManager.default,
  timeout: Duration = 60.seconds,
  isReadOnly: Boolean = false
) {
  @volatile private[this] var lastZxid: Int = 0
  @volatile private[this] var sessionId: Int = 0
  @volatile private[this] var sessionPasswd: Array[Byte] = new Array[Byte](16)

  // TODO: proper connection management
  private[this] def connReq = ConnectRequest(0, lastZxid, timeout, sessionId, sessionPasswd)
  private[this] val start = StartDispatcher(watchManager, timeout, isReadOnly, connReq)
  private[this] val svc: Future[Service[ZkRequest, Packet]] = factory() flatMap { svc =>
    svc(start) flatMap { rep: ConnectResponse =>
      sessionId = rep.sessionId
      sessionPasswd = rep.passwd
      // TODO: set negotiated timeout
      svc
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

  def create(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode): Future[String] = {
    val req = PacketRequest(OpCode.Create, CreateRequest(path, data, acl, 0), CreateResponse.unapply)
    write[CreateResponse](req) map { _.path }
  }

  def delete(path: String, version: Int): Future[Unit] = {
    write[DeleteResponse](PacketRequest(OpCode.Delete, DeleteRequest(path, version), DeleteResponse.unapply)).unit
  }

  def exists(path: String, watch: Boolean = false): Future[Stat] = {
    val watchFuture = if (watch) Some(watchManager.watchExists(path)) else None

    val req = PacketRequest(OpCode.Exists, ExistsRequest(path, watch), ExistsResponse.unapply)
    write[ExistsResponse](req) handle {
      case Return(ExistsResponse(stat)) => Future.value(ExistsResponse.NodeStat(stat, watchFuture))
      case Throw(KeeperException.NoNode) => Future.value(ExistsResponse.NoNode(watchFuture)
    }
  }

  def getACL(path: String): Future[(Stat, Seq[ACL])] = {
    val req = PacketRequest(OpCode.GetACL, GetACL2Request(path), GetACL2Response.unapply)
    write[GetACL2Response](req) map { rep => (rep.stat, rep.acl) }
  }

  def watchChildren(path: String): Future[WatchedEvent] = {
    val p = new Promise[WatchedEvent]
    getChildren(path, Some(p)) flatMap { _ => p }
  }

  def getChildren(path: String, watch: Option[Promise[WatchedEvent]] = None): Future[(Stat, Seq[String])] = {
    watch foreach watchManager.addWatch(path)
    val req = PacketRequest(OpCode.GetChildren, GetChildren2Request(path, watch.isDefined), GetChildren2Response.unapply)
    write[GetChildren2Response](req) map { rep => (rep.stat, rep.children) }
  }

  def watchData(path: String): Future[WatchedEvent] = {
    val p = new Promise[WatcheEvent]
    getData(data, Some(p)) flatMap { _ => p }
  }

  def getData(path: String, watch: Option[Promise[WatchedEvent]] = None): Future[(Stat, Array[Byte])] = {
    watch foreach watchManager.addWatch(path)
    val req = PacketRequest(OpCode.GetData, GetData2Request(path, watch.isDefined), GetData2Response.unapply)
    write[GetData2Response](req) map { rep => (rep.stat, rep.data) }
  }

  def setACL(path: String, acl: Seq[ACL], version: Int): Future[Stat] = {
    val req = PacketRequest(OpCode.SetACL, SetACLRequest(path, acl, version), SetACLResponse.unapply)
    write[SetACLResponse](req) map { _.stat }
  }

  def setData(path: String, data: Array[Byte], version: Int): Future[Stat] = {
    val req = PacketRequest(OpCode.SetData, SetDataRequest(path, data, version), SetDataResponse.unapply)
    write[SetDataResponse](req) map { _.stat }
  }

  //def multi(ops: Seq[Op]): Future[Seq[OpResult]]
}
