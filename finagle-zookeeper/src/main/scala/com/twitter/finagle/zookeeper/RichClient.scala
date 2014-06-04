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
  chroot: Option[String] = None
) {
  private[this] val watchManager = new WatchManager(checkWatch, chroot)

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

  private[this] def write[T <: Packet](req: RequestPacket[T]): Future[T] =
    dispatcher flatMap { svc =>
      svc(PacketRequest(req)) flatMap {
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

  private[this] def checkWatch(typ: WatchType, path: String): Future[Unit] =
    write(CheckWatchesRequest(path, typ.value)).unit

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

  private[this] def prependChroot(path: String): String =
    chroot map { c => if (path == "/") c else c + path } getOrElse(path)

  private[this] def handlePathIn(path: String): Future[String] =
    validatePath(path) map { _ => prependChroot(path) }

  /**
   * Create a node with the given path. The node data will be the given data,
   * and node acl will be the given acl.
   * <p>
   * The createMode argument specifies whether the created node will be ephemeral
   * or not.
   * <p>
   * An ephemeral node will be removed by the ZooKeeper automatically when the
   * session associated with the creation of the node expires.
   * <p>
   * The createMode argument can also specify to create a sequential node. The
   * actual path name of a sequential node will be the given path plus a
   * suffix "i" where i is the current sequential number of the node. The sequence
   * number is always fixed length of 10 digits, 0 padded. Once
   * such a node is created, the sequential number will be incremented by one.
   * <p>
   * If a node with the same actual path already exists in the ZooKeeper,
   * the Future will fail with a KeeperException with error code
   * KeeperException.NodeExists. Note that since a different actual path is used
   * for each invocation of creating sequential nodes with the same path argument,
   * the call will never fail with a KeeperException.NodeExists.
   * <p>
   * If the parent node does not exist in the ZooKeeper, the Future will fail with
   * KeeperException.NoNode.
   * <p>
   * An ephemeral node cannot have children. If the parent node of the given
   * path is ephemeral, the Future with fail with KeeperException.NoChildrenForEphemerals
   * <p>
   * This operation, if successful, will trigger all the watches left on the
   * node of the given path by exists and getData API calls, and the watches
   * left on the parent node by getChildren API calls.
   * <p>
   * If a node is created successfully, the ZooKeeper server will trigger the
   * watches on the path left by exists calls, and the watches on the parent
   * of the node by getChildren calls.
   * <p>
   * The maximum allowable size of the data is 1 MB (1,048,576 bytes).
   * Data larger than this will cause a KeeperExecption.
   *
   * @param path the path for the node
   * @param data the initial data for the node
   * @param acl the acl for the node
   * @param createMode specifying whether the node to be created is ephemeral and/or sequential
   */
  def create(
    path: String,
    data: Buf = Buf.Empty,
    acl: Seq[ACL] = Ids.OpenAclUnsafe,
    createMode: CreateMode = CreateMode.Persistent
  ): Future[String] = handlePathIn(path) flatMap { path =>
    val bytes = new Array[Byte](data.length)
    data.write(bytes, 0)
    write(CreateRequest(path, bytes, acl, createMode.flag)) map { _.path }
  }

  /**
   * Delete the node with the given path. The call will succeed if such a node
   * exists, and the given version matches the node's version (if the given
   * version is -1, it matches any node's versions).
   * <p>
   * The call will fail with KeeperException.NoNode if the node does not exist.
   * <p>
   * The call will fail with KeeperException.BadVersion if the given version
   * does not match the node's version.
   * <p>
   * The call will fail with KeeperException.NotEmpty if the node has children.
   * <p>
   * This operation, if successful, will trigger all the watches on the node
   * of the given path left by exists API calls, and the watches on the parent
   * node left by getChildren API calls.
   *
   * @param path the path of the node to be deleted.
   * @param version the expected node version.
   */
  def delete(path: String, version: Int): Future[Unit] = handlePathIn(path) flatMap { path =>
    write(DeleteRequest(path, version)).unit
  }

  /**
   * Return ExistsResponse.NodeStat with the node's stat if it exists. Return
   * ExistsResponse.NoNode if the node does not exist.
   * <p>
   * If watch is set to true both the ExistsResponse will contain a watch that
   * will be triggered by a successful operation that creates/delete the node
   * or sets the data on the node.
   *
   * @param path the node path
   * @param watcher whether to watch this node
   */
  def exists(path: String, watch: Boolean = false): Future[ExistsResponse] = handlePathIn(path) flatMap { path =>
    val watcher = if (watch) Some(watchManager.addWatch(WatchType.Data, path)) else None

    write[ExistsResponsePacket](ExistsRequest(path, watch)) transform {
      case Return(ExistsResponsePacket(stat)) => Future.value(ExistsResponse.NodeStat(stat, watcher))
      case Throw(KeeperException.NoNode) => Future.value(ExistsResponse.NoNode(watcher))
    }
  }

  /**
   * Return the ACL and stat in a GetACLResponse of the node of the given path.
   * <p>
   * If the node does not exist the Future will fail with KeeperException.NoNode.
   *
   * @param path the given path for the node
   */
  def getACL(path: String): Future[GetACLResponse] = handlePathIn(path) flatMap { path =>
    write(GetACLRequest(path)) map { rep => GetACLResponse(rep.stat, rep.acl) }
  }

  /**
   * Return the list of the children and stat in a GetChildrenResponse of the
   * node of the given path.
   * <p>
   * If the watch is true and the call is successful (no exception is returned),
   * a watch will be left on the node with the given path and set in the
   * GetChildrenResponse. The watch will be triggered by a successful operation
   * that deletes the node of the given path or creates/delete a child under the node.
   * <p>
   * The list of children returned is not sorted and no guarantee is provided
   * as to its natural or lexical order.
   * <p>
   * The call will fail with KeeperException.NoNode if the node does not exist.
   *
   * @param path the node path
   * @param watcher whether to watch this node
   */
  def getChildren(path: String, watch: Boolean = false): Future[GetChildrenResponse] = handlePathIn(path) flatMap { path =>
    val watcher = if (watch) Some(watchManager.addWatch(WatchType.Children, path)) else None
    write(GetChildren2Request(path, watch)) map { rep => GetChildrenResponse(rep.stat, rep.children, watcher) }
  }

  /**
   * Return the data and the stat in a GetDataResponse of the node of the given
   * path.
   * <p>
   * If watch is true and the call is successful (no exception is returned),
   * a watch will be left on the node with the given path and set in the
   * GetDataResponse. The watch will be triggered by a successful operation
   * that sets data on the node, or deletes the node.
   * <p>
   * The call will fail with KeeperException.NoNode if the node does not exist.
   *
   * @param path the node path
   * @param watcher whether to watch this node
   */
  def getData(path: String, watch: Boolean = false): Future[GetDataResponse] = handlePathIn(path) flatMap { path =>
    val watcher = if (watch) Some(watchManager.addWatch(WatchType.Data, path)) else None
    write(GetDataRequest(path, watch)) map { rep => GetDataResponse(rep.stat, Buf.ByteArray(rep.data), watcher) }
  }

  /**
   * Set the ACL for the node of the given path if such a node exists and the
   * given version matches the version of the node. Return the stat of the
   * node.
   * <p>
   * The call will fail with KeeperException.NoNode if the node does not exist.
   * <p>
   * The call will fail with KeeperException.BadVersion if the given version
   * does not match the node's version.
   *
   * @param path the path of the node to be deleted.
   * @param acl the acl for the node
   * @param version the expected node version.
   */
  def setACL(path: String, acl: Seq[ACL], version: Int): Future[Stat] = handlePathIn(path) flatMap { path =>
    write(SetACLRequest(path, acl, version)) map { _.stat }
  }

  /**
   * Set the data for the node of the given path if such a node exists and the
   * given version matches the version of the node (if the given version is
   * -1, it matches any node's versions). Return the stat of the node.
   * <p>
   * This operation, if successful, will trigger all the watches on the node
   * of the given path left by getData calls.
   * <p>
   * The call will fail with KeeperException.NoNode if the node does not exist.
   * <p>
   * The call will fail with KeeperException.BadVersion if the given version
   * does not match the node's version.
   * <p>
   * The maximum allowable size of the data is 1 MB (1,048,576 bytes).
   * Data larger than this will cause a KeeperExecption.
   *
   * @param path the path of the node
   * @param data the data to set
   * @param version the expected matching version
   */
  def setData(path: String, data: Buf, version: Int): Future[Stat] = handlePathIn(path) flatMap { path =>
    val bytes = new Array[Byte](data.length)
    data.write(bytes, 0)
    write(SetDataRequest(path, bytes, version)) map { _.stat }
  }

  /**
   * For the given znode path, removes all the registered watchers of given
   * watcherType.
   *
   * <p>
   * A successful call guarantees that, the removed watchers won't be
   * triggered.
   * </p>
   *
   * @param path the path of the node
   * @param watcherType the type of watcher to be removed
   * @param local whether watches can be removed locally when there is no server connection
   */
  def removeAllWatches(typ: WatchType, path: String): Future[Unit] = handlePathIn(path) flatMap { path =>
    write(RemoveWatchesRequest(path, typ.value)).unit
  }

  // TODO: re-work the docs (maybe the exception, too)
  // TODO: support chroot
  /**
   * Executes multiple ZooKeeper operations or none of them.
   * <p>
   * On success, a list of results is returned.
   * On failure, an exception is raised which contains partial results and
   * error details, see {@link KeeperException#getResults}
   * <p>
   * Note: The maximum allowable size of all of the data arrays in all of
   * the setData operations in this single request is typically 1 MB
   * (1,048,576 bytes). This limit is specified on the server via
   * <a href="http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#Unsafe+Options">jute.maxbuffer</a>.
   * Requests larger than this will cause a KeeperException to be
   * thrown.
   *
   * @param ops An iterable that contains the operations to be done.
   * These should be created using the factory methods on {@link Op}.
   * @return A list of results, one for each input Op, the order of
   * which exactly matches the order of the <code>ops</code> input
   * operations.
   */
  def multi(ops: Seq[Multi.Op]): Future[Seq[Multi.OpResult]] =
    write(MultiRequest(ops)) map { _.results }
}
