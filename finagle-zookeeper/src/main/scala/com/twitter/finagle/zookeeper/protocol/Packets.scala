package com.twitter.finagle.zookeeper.protocol

sealed trait Packet {
  def buf: Buf
}

case class Id(scheme: String, id: String) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(scheme))
    .concat(BufString(id))
}

object Id {
  def unapply(buf: Buf): Option[(Id, Buf)] = {
    val BufString(scheme, rem0) = buf
    val BufString(id, rem1) = rem0
    Some((new Id(scheme, id), rem1)
  }
}

case class ACL(perms: Int, id: Id) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(perms))
    .concat(id.buf)
}

object ACL {
  def unapply(buf: Buf): Option[(ACL, Buf)] = {
    val BufInt(perms, rem0) = buf
    val Id(id, rem1) = rem0
    Some((new ACL(perms, id), rem1))
  }
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
  def buf: Buf = Buf.Empty
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

object Stat {
  def unapply(buf: Buf): Option[(Stat, Buf)] = {
    val BufLong(czxid, rem0) = buf
    val BufLong(mzxid, rem1) = rem0
    val BufLong(ctime, rem2) = rem1
    val BufLong(mtime, rem3) = rem2
    val BufInt(version, rem4) = rem3
    val BufInt(cversion, rem5) = rem4
    val BufInt(aversion, rem6) = rem5
    val BufLong(ephemeralOwner, rem7) = rem6
    val BufInt(dataLength, rem8) = rem7
    val BufInt(numChildren, rem9) = rem8
    val BufLong(pzxid, rem10) = rem9
    Some((
      new Stat(czxid, mzcid, ctime, mtime, version, cversion, aversion, ephermalOwner, dataLength, numChildren, pzxid),
      rem10))
  }
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
  def buf: Buf = Buf.Empty
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

object StatPersisted {
  def unapply(buf: Buf): Option[(StatPersisted, Buf)] = {
    val BufLong(czxid, rem0) = buf
    val BufLong(mzxid, rem1) = rem0
    val BufLong(ctime, rem2) = rem1
    val BufLong(mtime, rem3) = rem2
    val BufInt(version, rem4) = rem3
    val BufInt(cversion, rem5) = rem4
    val BufInt(aversion, rem6) = rem5
    val BufLong(ephemeralOwner, rem7) = rem6
    val BufLong(pzxid, rem8) = rem7
    Some((
      new StatPersisted(czxid, mzcid, ctime, mtime, version, cversion, aversion, ephermalOwner, pzxid),
      rem8))
  }
}

case class ConnectRequest(
  protocolVersion: Int,
  lastZxidSeen: Long,
  timeOut: Int,
  sessionId: Long,
  passwd: Array[Byte]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(protocolVersion))
    .concat(BufLong(lastZxidSeen))
    .concat(BufInt(timeOut))
    .concat(BufLong(sessionId))
    .concat(BufArray(passwd))
}

object ConnectRequest {
  def unapply(buf: Buf): Option[(ConnectRequest, Buf)] = {
    val BufInt(protocolVersion, rem0) = buf
    val BufLong(lastZxidSeen, rem1) = rem0
    val BufInt(timeOut, rem2) = rem1
    val BufLong(sessionId, rem3) = rem2
    val BufArray(passwd, rem4) = rem3
    Some((
      ConnectRequest(protocolVersion, lastZxidSeen, timeOut, sessionId, passwd),
      rem4))
  }
}

case class ConnectResponse(
  protocolVersion: Int,
  timeOut: Int,
  sessionId: Long,
  passwd: String
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(protocolVersion))
    .concat(BufInt(timeOut))
    .concat(BufLong(sessionId))
    .concat(BufArray(passwd))
}

object ConnectResponse {
  def unapply(buf: Buf): Option[(ConnectResponse, Buf)] = {
    val BufInt(protocolVersion, rem0) = buf
    val BufInt(timeOut, rem1) = rem0
    val BufLong(sessionId, rem2) = rem1
    val BufArray(passwd, rem3) = rem2
    Some((
      ConnectResponse(protocolVersion, timeOut, sessionId, passwd),
      rem3))
  }
}

case class SetWatches(
  relativeZxid: Long,
  dataWatches: Seq[String],
  existWatches: Seq[String],
  childWatches: Seq[String]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufLong(relativeZxid))
    .concat(BufSeq(dataWatches, BufString.apply))
    .concat(BufSeq(existWatches, BufString.appy))
    .concat(BufSeq(childWatches, BufString.appy))
}

object SetWatches {
  def unapply(buf: Buf): Option[(SetWatches, Buf)] = {
    val BufLong(relativeZxid, rem0) = buf
    val BufSeq(dataWatches, rem1) = (rem0, BufString.unapply)
    val BufSeq(existWatches, rem2) = (rem1, BufString.unapply)
    val BufSeq(childWatches, rem3) = (rem2, BufString.unapply)
    Some((SetWatches(relativeZxid, dataWatches, existWatches, childWatches), rem3))
  }
}

case class RequestHeader(
  xid: Int,
  typ: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(xid))
    .concat(BufInt(typ))
}

object RequestHeader {
  def unapply(buf: Buf): Option[(RequestHeader, Buf)] = {
    val BufInt(xid, rem0) = buf
    val BufInt(typ, rem1) = rem0
    Some((RequestHeader(xid, typ), rem1))
  }
}

case class MultiHeader(
  typ: Int,
  done: Boolean,
  err: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(typ))
    .concat(BufBool(done))
    .concat(BufInt(err))
}

object MultiHeader {
  def unapply(buf: Buf): Option[(MultiHeader , Buf)] = {
    val BufInt(typ, rem0) = buf
    val BufBool(done, rem1) = rem0
    val BufInt(err, rem2) = rem1
    Some((MultiHeader(typ, done, err), rem2))
  }
}

case class AuthPacket(
  typ: Int,
  scheme: String,
  auth: Array[Byte]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(typ))
    .concat(BufString(scheme))
    .concat(BufArray(auth))
}

case class GetDataRequest(
  path: String,
  watch: Boolean
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))

}

case class SetDataRequest(
  path: String,
  data: Array[Byte],
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufInt(version))
}

case class ReconfigRequest(
  joiningServers: String
  leavingServers: String
  newMembers: String
  curConfigId: Long
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(joiningServers))
    .concat(BufString(leavingServers))
    .concat(BufString(newMembers))
    .concat(BufLong(curConfigId))
}

case class SetDataResponse(
  stat: Stat
) extends Packet {
  def buf: Buf = stat.buf
}

case class GetSASLRequest(
  token: Array[Byte]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufArray(token))
}

case class SetSASLRequest(
  token: Array[Byte]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufArray(token))
}

case class SetSASLResponse(
  token: Array[Byte]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufArray(token))
}

case class CreateRequest(
  path: String,
  data: Array[Byte],
  acl: Seq[ACL],
  flags: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(acl.foldLeft(BufInt(acl.size))((b,a) => b.concat(a.buf)))
    .concat(BufInt(flags))
}

case class CreateRequest(
  path: String,
  data: Array[Byte],
  acl: Seq[ACL],
  flags: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(acl.foldLeft(BufInt(acl.size))((b,a) => b.concat(a.buf)))
    .concat(BufInt(flags))
}

case class DeleteRequest(
  path: String,
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(version))
}

case class GetChildrenRequest(
  path: String,
  watch: Boolean
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

case class GetChildren2Request(
  path: String,
  watch: Boolean
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

case class CheckVersionRequest(
  path: String,
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(version))
}

case class GetMaxChildrenRequest(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

case class GetMaxChildrenResponse(
  max: Int
) extends Packet {
  def buf: Buf = BufInt(max)
}

case class SetMaxChildrenRequest(
  path: String,
  max: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(max))
}

case class SyncRequest(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

case class SyncResponse(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

case class GetACLRequest(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

case class SetACLRequest(
  path: String,
  acl: Seq[ACL],
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(acl.foldLeft(BufInt(acl.size))((b,a) => b.concat(a.buf)))
    .concat(BufInt(version)
}

case class SetACLResponse(
  stat: Stat
) extends Packet {
  def buf: Buf = stat.buf
}

case class WatcherEvent(
  typ: Int,
  state: Int,
  path: String
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(typ))
    .concat(BufInt(state))
    .concat(BufString(path))
}

case class ErrorResponse(
  err: Int
) extends Packet {
  def buf: Buf = BufInt(err)
}

case class CreateResponse(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

case class Create2Response(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

case class ExistsRequest(
  path: String,
  watch: Boolean
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch)
}

case class ExistsResponse(
  stat: Stat
) extends Packet {
  def buf: Buf = stat.buf
}
case class GetDataResponse(
  data: Array[Byte],
  stat: Stat
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufArray(data))
    .concat(stat.buf)
}

case class GetChildrenResponse(
  children: Seq[String]
) extends Packet {
  def buf: Buf = children.foldLeft(BufInt(children.size))((b,c) => b.concat(BufString(c)))
}

case class GetChildren2Response(
  children: Seq[String],
  stat: Stat
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(children.foldLeft(BufInt(children.size))((b,c) => b.concat(BufString(c))))
    .concat(stat.buf)
}

case class GetACLResponse(
  acl: Seq[ACL],
  stat: Stat
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(acl.foldLeft(BufInt(acl.size))((b,a) => b.concat(a.buf)))
    .concat(stat.buf)
}

case class CheckWatchesRequest(
  path: String,
  typ: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(typ))
}

case class RemoveWatchesRequest(
  path: String,
  typ: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(typ))
}

case class LearnerInfo(
  serverid: Long,
  protocolVersion: Int,
  configVersion: Long
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufLong(serverid))
    .concat(BufInt(protocolVersion))
    .concat(BufLong(configVersion))
}

case class QuorumPacket(
  typ: Int,
  zxid: Long,
  data: Array[Byte],
  authinfo: Seq[Id]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(typ))
    .concat(BufLong(zxid))
    .concat(BufArray(data))
    .concat(authinfo.foldLeft(BufInt(authinfo.size))((b,i) => b.concat(i.buf)))
}

case class FileHeader(
  magic: Int,
  version: Int,
  dbid: Long
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(magic))
    .concat(BufInt(version))
    .concat(BufLong(dbid))
}

case class TxnHeader(
  clientId: Long,
  cxid: Int,
  zxid: Long,
  time: Long,
  typ: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufLong(clientId))
    .concat(BufInt(cxid))
    .concat(BufLong(zxid))
    .concat(BufLong(time))
    .concat(BufInt(typ))
}

case class CreateTxnV0(
  path: String,
  data: Array[Byte],
  acl: Seq[ACL],
  ephemeral: Boolean
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(acl.foldLeft(BufInt(acl.size))((b,a) => b.concat(a.buf)))
    .concat(BufBool(ephemeral))
}

case class CreateTxn(
  path: String,
  data: Array[Byte],
  acl: Seq[ACL],
  ephemeral: Boolean,
  parentCVersion: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(acl.foldLeft(BufInt(acl.size))((b,a) => b.concat(a.buf)))
    .concat(BufBool(ephemeral))
    .concat(BufInt(parentCVersion))
}

case class DeleteTxt(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

case class SetDataTxn(
  path: String,
  data: Array[Byte],
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufInt(version))
}

case class CheckVersionTxn(
  path: String,
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(version))
}

case class SetACLTxn(
  path: String,
  acl: Seq[ACL],
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(acl.foldLeft(BufInt(acl.size))((b,a) => b.concat(a.buf)))
    .concat(BufInt(version))
}

case class SetMaxChildrenTxn(
  path: String,
  max: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(max))
}

case class CreateSessionTxn(
  timeOut: Int
) extends Packet {
  def buf: Buf = BufInt(timeOut)
}

case class ErrorTxn(
  err: Int
) extends Packet {
  def buf: Buf = BufInt(err)
}

case class Txn(
  typ: Int,
  data: Array[Byte]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(typ))
    .concat(BufArray(data))
}

case class MultiTxn(
  txns: Seq[Txn]
) extends Packet {
  def buf: Buf = txns.foldLeft(BufInt(txns.size))((b,t) => b.concat(t.buf))
}
