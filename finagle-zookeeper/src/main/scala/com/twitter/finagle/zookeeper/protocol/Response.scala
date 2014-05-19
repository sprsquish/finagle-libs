case class Id(scheme: String, id: String) extends Packet {
  def serialized: Buf =
    Buf.Empty
      .concat(BufString(scheme))
      .concat(BufString(id))
}

case class ACL(perms: Int, id: Id) extends Packet {
  def serialized: Buf =
    Buf.Empty
      .concat(BufInt(perms))
      .concat(id.buf)
}

case class Stat(
  czxid: Long,
  mzxid: Long,
  ctime: Long,
  mtime: Long,
  version: Int,
  cversion: Int,
  aversion: Int,
  ephemeralOwner: Long,
  dataLength: Int,
  numChildren: Int,
  pzxid: Long
) extends Packet {
  def serialized: Buf =
    Buf.Empty
      .concat(BufLong(czxid))
      .concat(BufLong(mzxid))
      .concat(BufLong(ctime))
      .concat(BufLong(mtime))
      .concat(BufInt(version))
      .concat(BufInt(cversion))
      .concat(BufInt(aversion))
      .concat(BufLong(ephemeralOwner))
      .concat(BufInt(dataLength))
      .concat(BufInt(numChildren))
      .concat(BufLong(pzxid))
}

case class StatPersisted(
  czxid: Long,
  mzxid: Long,
  ctime: Long,
  mtime: Long
  version: Int,
  cversion: Int,
  aversion: Int,
  ephemeralOwner: Long,
  pzxid: Long
) extends Packet {
  def serialized: Buf =
    Buf.Empty
      .concat(BufLong(czxid))
      .concat(BufLong(mzxid))
      .concat(BufLong(ctime))
      .concat(BufLong(mtime))
      .concat(BufInt(version))
      .concat(BufInt(cversion))
      .concat(BufInt(aversion))
      .concat(BufLong(ephemeralOwner))
      .concat(BufLong(pzxid))
}

case class ConnectRequest(
  protocolVersion: Int,
  lastZxidSeen: Long,
  timeOut: Int,
  sessionId: Long,
  passwd: Array[Byte]
) extends Packet {
  def serialized: Buf =
    Buf.Empty
      .concat(BufInt(protocolVersion))
      .concat(BufLong(lastZxidSeen))
      .concat(BufInt(timeOut))
      .concat(BufLong(sessionId))
      .concat(BufArray(passwd))
}

case class ConnectResponse(
  protocolVersion: Int,
  timeOut: Int,
  sessionId: Long,
  passwd: String
) extends Packet {
  def serialized: Buf =
    Buf.Empty
      .concat(BufInt(protocolVersion))
      .concat(BufInt(timeOut))
      .concat(BufLong(sessionId))
      .concat(BufArray(passwd))
}

case class SetWatches(
  xid: Int,
  relativeZxid: Long,
  dataWatches: Seq[String],
  existWatches: Seq[String],
  childWatches: Seq[String]
) extends Packet {
  def serialize: Buf = {
    val buf = Buf.Empty
      .concat(BufInt(xid))
      .concat(BufInt(OpCode.SetWatches))
      .concat(dataWatches.foldLeft(Buf.Empty) { (b: Buf, w) => b.concat(BufString(w)) })
      .concat(existWatches.foldLeft(Buf.Empty) { (b: Buf, w) => b.concat(BufString(w)) })
      .concat(childWatches.foldLeft(Buf.Empty) { (b: Buf, w) => b.concat(BufString(w)) })
}

module org.apache.zookeeper.proto {
    class ConnectRequest {
        int protocolVersion;
        long lastZxidSeen;
        int timeOut;
        long sessionId;
        buffer passwd;
    }
    class ConnectResponse {
        int protocolVersion;
        int timeOut;
        long sessionId;
        buffer passwd;
    }
    class SetWatches {
        long relativeZxid;
        vector<ustring>dataWatches;
        vector<ustring>existWatches;
        vector<ustring>childWatches;
    }
    class RequestHeader {
        int xid;
        int type;
    }
    class MultiHeader {
        int type;
        boolean done;
        int err;
    }
    class AuthPacket {
        int type;
        ustring scheme;
        buffer auth;
    }
    class ReplyHeader {
        int xid;
        long zxid;
        int err;
    }

    class GetDataRequest {
        ustring path;
        boolean watch;
    }

    class SetDataRequest {
        ustring path;
        buffer data;
        int version;
    }
    class ReconfigRequest {
        ustring joiningServers;
        ustring leavingServers;
        ustring newMembers;
        long curConfigId;
    }
    class SetDataResponse {
        org.apache.zookeeper.data.Stat stat;
    }
    class GetSASLRequest {
        buffer token;
    }
    class SetSASLRequest {
        buffer token;
    }
    class SetSASLResponse {
        buffer token;
    }
    class CreateRequest {
        ustring path;
        buffer data;
        vector<org.apache.zookeeper.data.ACL> acl;
        int flags;
    }
    class Create2Request {
        ustring path;
        buffer data;
        vector<org.apache.zookeeper.data.ACL> acl;
        int flags;
    }
    class DeleteRequest {
        ustring path;
        int version;
    }
    class GetChildrenRequest {
        ustring path;
        boolean watch;
    }
    class GetChildren2Request {
        ustring path;
        boolean watch;
    }
    class CheckVersionRequest {
        ustring path;
        int version;
    }
    class GetMaxChildrenRequest {
        ustring path;
    }
    class GetMaxChildrenResponse {
        int max;
    }
    class SetMaxChildrenRequest {
        ustring path;
        int max;
    }
    class SyncRequest {
        ustring path;
    }
    class SyncResponse {
        ustring path;
    }
    class GetACLRequest {
        ustring path;
    }
    class SetACLRequest {
        ustring path;
        vector<org.apache.zookeeper.data.ACL> acl;
        int version;
    }
    class SetACLResponse {
        org.apache.zookeeper.data.Stat stat;
    }
    class WatcherEvent {
        int type;  // event type
        int state; // state of the Keeper client runtime
        ustring path;
    }
    class ErrorResponse {
        int err;
    }
    class CreateResponse {
        ustring path;
    }
    class Create2Response {
        ustring path;
        org.apache.zookeeper.data.Stat stat;
    }
    class ExistsRequest {
        ustring path;
        boolean watch;
    }
    class ExistsResponse {
        org.apache.zookeeper.data.Stat stat;
    }
    class GetDataResponse {
        buffer data;
        org.apache.zookeeper.data.Stat stat;
    }
    class GetChildrenResponse {
        vector<ustring> children;
    }
    class GetChildren2Response {
        vector<ustring> children;
        org.apache.zookeeper.data.Stat stat;
    }
    class GetACLResponse {
        vector<org.apache.zookeeper.data.ACL> acl;
        org.apache.zookeeper.data.Stat stat;
    }
    class CheckWatchesRequest {
        ustring path;
        int type;
    }
    class RemoveWatchesRequest {
        ustring path;
        int type;
    }
}

module org.apache.zookeeper.server.quorum {
    class LearnerInfo {
        long serverid;
        int protocolVersion;
        long configVersion;
    }
    class QuorumPacket {
        int type; // Request, Ack, Commit, Ping
        long zxid;
        buffer data; // Only significant when type is request
        vector<org.apache.zookeeper.data.Id> authinfo;
    }
}

module org.apache.zookeeper.server.persistence {
    class FileHeader {
        int magic;
        int version;
        long dbid;
    }
}

module org.apache.zookeeper.txn {
    class TxnHeader {
        long clientId;
        int cxid;
        long zxid;
        long time;
        int type;
    }
    class CreateTxnV0 {
        ustring path;
        buffer data;
        vector<org.apache.zookeeper.data.ACL> acl;
        boolean ephemeral;
    }
    class CreateTxn {
        ustring path;
        buffer data;
        vector<org.apache.zookeeper.data.ACL> acl;
        boolean ephemeral;
        int parentCVersion;
    }
    class DeleteTxn {
        ustring path;
    }
    class SetDataTxn {
        ustring path;
        buffer data;
        int version;
    }
    class CheckVersionTxn {
        ustring path;
        int version;
    }
    class SetACLTxn {
        ustring path;
        vector<org.apache.zookeeper.data.ACL> acl;
        int version;
    }
    class SetMaxChildrenTxn {
        ustring path;
        int max;
    }
    class CreateSessionTxn {
        int timeOut;
    }
    class ErrorTxn {
        int err;
    }
    class Txn {
        int type;
        buffer data;
    }
    class MultiTxn {
        vector<org.apache.zookeeper.txn.Txn> txns;
    }
}

