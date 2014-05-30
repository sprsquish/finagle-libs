package com.twitter.finagle.zookeeper.protocol

import com.twitter.io.Buf

sealed trait Packet {
  def buf: Buf
}

// Special Case Packets

trait EmptyPacket extends Packet { def buf: Buf = Buf.Empty }
object EmptyPacket extends EmptyPacket


// Protocol Packets

case class Id(scheme: String, id: String) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(scheme))
    .concat(BufString(id))
}

object Id {
  def unapply(buf: Buf): Option[(Id, Buf)] = {
    val BufString(scheme, BufString(id, rem)) = buf
    Some(Id(scheme, id), rem)
  }
}

case class ACL(perms: Int, id: Id) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(perms))
    .concat(id.buf)
}

object ACL {
  def unapply(buf: Buf): Option[(ACL, Buf)] = {
    val BufInt(perms, Id(id, rem)) = buf
    Some(ACL(perms, id), rem)
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
    val BufLong(czxid,
        BufLong(mzxid,
        BufLong(ctime,
        BufLong(mtime,
        BufInt(version,
        BufInt(cversion,
        BufInt(aversion,
        BufLong(ephemeralOwner,
        BufInt(dataLength,
        BufInt(numChildren,
        BufLong(pzxid,
        rem
    ))))))))))) = buf
    Some(Stat(czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeralOwner, dataLength, numChildren, pzxid), rem)
  }
}

case class StatPersisted(
  czxid: Long,
  mzxid: Long,
  ctime: Long,
  mtime: Long,
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
    val BufLong(czxid,
        BufLong(mzxid,
        BufLong(ctime,
        BufLong(mtime,
        BufInt(version,
        BufInt(cversion,
        BufInt(aversion,
        BufLong(ephemeralOwner,
        BufLong(pzxid,
        rem
    ))))))))) = buf
    Some(StatPersisted(czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeralOwner, pzxid), rem)
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
    val BufInt(protocolVersion,
        BufLong(lastZxidSeen,
        BufInt(timeOut,
        BufLong(sessionId,
        BufArray(passwd,
        rem
    ))))) = buf
    Some(ConnectRequest(protocolVersion, lastZxidSeen, timeOut, sessionId, passwd), rem)
  }
}

case class ConnectResponse(
  protocolVersion: Int,
  timeOut: Int,
  sessionId: Long,
  passwd: Array[Byte]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(protocolVersion))
    .concat(BufInt(timeOut))
    .concat(BufLong(sessionId))
    .concat(BufArray(passwd))
}

object ConnectResponse {
  def unapply(buf: Buf): Option[(ConnectResponse, Buf)] = {
    val BufInt(protocolVersion,
        BufInt(timeOut,
        BufLong(sessionId,
        BufArray(passwd,
        rem
    )))) = buf
    Some(ConnectResponse(protocolVersion, timeOut, sessionId, passwd), rem)
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
    .concat(BufSeqString(dataWatches))
    .concat(BufSeqString(existWatches))
    .concat(BufSeqString(childWatches))
}

object SetWatches {
  def unapply(buf: Buf): Option[(SetWatches, Buf)] = {
    val BufLong(relativeZxid,
        BufSeqString(dataWatches,
        BufSeqString(existWatches,
        BufSeqString(childWatches,
        rem
    )))) = buf
    Some(SetWatches(relativeZxid, dataWatches, existWatches, childWatches), rem)
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
    val BufInt(xid, BufInt(typ, rem)) = buf
    Some(RequestHeader(xid, typ), rem)
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
  def unapply(buf: Buf): Option[(MultiHeader, Buf)] = {
    val BufInt(typ, BufBool(done, BufInt(err, rem))) = buf
    Some(MultiHeader(typ, done, err), rem)
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

object AuthPacket {
  def unapply(buf: Buf): Option[(AuthPacket, Buf)] = {
    val BufInt(typ, BufString(scheme, BufArray(auth, rem))) = buf
    Some(AuthPacket(typ, scheme, auth), rem)
  }
}

case class ReplyHeader(
  xid: Int,
  zxid: Long,
  err: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(xid))
    .concat(BufLong(zxid))
    .concat(BufInt(err))
}

object ReplyHeader {
  def unapply(buf: Buf): Option[(ReplyHeader, Buf)] = {
    val BufInt(xid, BufLong(zxid, BufInt(err, rem))) = buf
    Some(ReplyHeader(xid, zxid, err), rem)
  }
}

case class GetDataRequest(
  path: String,
  watch: Boolean
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

object GetDataRequest {
  def unapply(buf: Buf): Option[(GetDataRequest, Buf)] = {
    val BufString(patch, BufBool(watch, rem)) = buf
    Some(GetDataRequest(patch, watch), rem)
  }
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

object SetDataRequest {
  def unapply(buf: Buf): Option[(SetDataRequest, Buf)] = {
    val BufString(path, BufArray(data, BufInt(version, rem))) = buf
    Some(SetDataRequest(path, data, version), rem)
  }
}

case class ReconfigRequest(
  joiningServers: String,
  leavingServers: String,
  newMembers: String,
  curConfigId: Long
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(joiningServers))
    .concat(BufString(leavingServers))
    .concat(BufString(newMembers))
    .concat(BufLong(curConfigId))
}

object ReconfigRequest {
  def unapply(buf: Buf): Option[(ReconfigRequest, Buf)] = {
    val BufString(joiningServers,
        BufString(leavingServers,
        BufString(newMembers,
        BufLong(curConfigId,
        rem
    )))) = buf
    Some(ReconfigRequest(joiningServers, leavingServers, newMembers, curConfigId), rem)
  }
}

case class SetDataResponse(
  stat: Stat
) extends Packet {
  def buf: Buf = stat.buf
}

object SetDataResponse {
  def unapply(buf: Buf): Option[(SetDataResponse, Buf)] = {
    val Stat(stat, rem) = buf
    Some(SetDataResponse(stat), rem)
  }
}

case class GetSASLRequest(
  token: Array[Byte]
) extends Packet {
  def buf: Buf = BufArray(token)
}

object GetSASLRequest {
  def unapply(buf: Buf): Option[(GetSASLRequest, Buf)] = {
    val BufArray(token, rem) = buf
    Some(GetSASLRequest(token), rem)
  }
}

case class SetSASLRequest(
  token: Array[Byte]
) extends Packet {
  def buf: Buf = BufArray(token)
}

object SetSASLRequest {
  def unapply(buf: Buf): Option[(SetSASLRequest, Buf)] = {
    val BufArray(token, rem) = buf
    Some(SetSASLRequest(token), rem)
  }
}

case class SetSASLResponse(
  token: Array[Byte]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufArray(token))
}

object SetSASLResponse {
  def unapply(buf: Buf): Option[(SetSASLResponse , Buf)] = {
    val BufArray(token, rem) = buf
    Some(SetSASLResponse(token), rem)
  }
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
    .concat(BufSeqACL(acl))
    .concat(BufInt(flags))
}

object CreateRequest {
  def unapply(buf: Buf): Option[(CreateRequest, Buf)] = {
    val BufString(path,
        BufArray(data,
        BufSeqACL(acl,
        BufInt(flags,
        rem
    )))) = buf
    Some(CreateRequest(path, data, acl, flags), rem)
  }
}

case class DeleteRequest(
  path: String,
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(version))
}

object DeleteRequest {
  def unapply(buf: Buf): Option[(DeleteRequest, Buf)] = {
    val BufString(path, BufInt(version, rem)) = buf
    Some(DeleteRequest(path, version), rem)
  }
}

// a fake response because finagle requires one
case class DeleteResponse() extends Packet {
  def buf: Buf = Buf.Empty
}

object DeleteResponse {
  def unapply(buf: Buf): Option[(DeleteResponse, Buf)] =
    Some(DeleteResponse(), buf)
}

case class GetChildrenRequest(
  path: String,
  watch: Boolean
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

object GetChildrenRequest {
  def unapply(buf: Buf): Option[(GetChildrenRequest, Buf)] = {
    val BufString(path, BufBool(watch, rem)) = buf
    Some(GetChildrenRequest(path, watch), rem)
  }
}

case class GetChildren2Request(
  path: String,
  watch: Boolean
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

object GetChildren2Request {
  def unapply(buf: Buf): Option[(GetChildren2Request, Buf)] = {
    val BufString(path, BufBool(watch, rem)) = buf
    Some(GetChildren2Request(path, watch), rem)
  }
}

case class CheckVersionRequest(
  path: String,
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(version))
}

object CheckVersionRequest {
  def unapply(buf: Buf): Option[(CheckVersionRequest, Buf)] = {
    val BufString(path, BufInt(watch, rem)) = buf
    Some(CheckVersionRequest(path, watch), rem)
  }
}

case class GetMaxChildrenRequest(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

object GetMaxChildrenRequest {
  def unapply(buf: Buf): Option[(GetMaxChildrenRequest, Buf)] = {
    val BufString(path, rem) = buf
    Some(GetMaxChildrenRequest(path), rem)
  }
}

case class GetMaxChildrenResponse(
  max: Int
) extends Packet {
  def buf: Buf = BufInt(max)
}

object GetMaxChildrenResponse {
  def unapply(buf: Buf): Option[(GetMaxChildrenResponse, Buf)] = {
    val BufInt(max, rem) = buf
    Some(GetMaxChildrenResponse(max), rem)
  }
}

case class SetMaxChildrenRequest(
  path: String,
  max: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(max))
}

object SetMaxChildrenRequest {
  def unapply(buf: Buf): Option[(SetMaxChildrenRequest, Buf)] = {
    val BufString(path, BufInt(max, rem)) = buf
    Some(SetMaxChildrenRequest(path, max), rem)
  }
}

case class SyncRequest(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

object SyncRequest {
  def unapply(buf: Buf): Option[(SyncRequest, Buf)] = {
    val BufString(path, rem) = buf
    Some(SyncRequest(path), rem)
  }
}

case class SyncResponse(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

object SyncResponse {
  def unapply(buf: Buf): Option[(SyncResponse, Buf)] = {
    val BufString(path, rem) = buf
    Some(SyncResponse(path), rem)
  }
}

case class GetACLRequest(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

object GetACLRequest {
  def unapply(buf: Buf): Option[(GetACLRequest, Buf)] = {
    val BufString(path, rem) = buf
    Some(GetACLRequest(path), rem)
  }
}

case class SetACLRequest(
  path: String,
  acl: Seq[ACL],
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufSeqACL(acl))
    .concat(BufInt(version))
}

object SetACLRequest {
  def unapply(buf: Buf): Option[(SetACLRequest, Buf)] = {
    val BufString(path, BufSeqACL(acl, BufInt(version, rem))) = buf
    Some(SetACLRequest(path, acl, version), rem)
  }
}

case class SetACLResponse(
  stat: Stat
) extends Packet {
  def buf: Buf = stat.buf
}

object SetACLResponse {
  def unapply(buf: Buf): Option[(SetACLResponse, Buf)] = {
    val Stat(stat, rem) = buf
    Some(SetACLResponse(stat), rem)
  }
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

object WatcherEvent {
  def unapply(buf: Buf): Option[(WatcherEvent, Buf)] = {
    val BufInt(typ, BufInt(state, BufString(path, rem))) = buf
    Some(WatcherEvent(typ, state, path), rem)
  }
}

case class ErrorResponse(
  err: Int
) extends Packet {
  def buf: Buf = BufInt(err)
}

object ErrorResponse {
  def unapply(buf: Buf): Option[(ErrorResponse, Buf)] = {
    val BufInt(err, rem) = buf
    Some(ErrorResponse(err), rem)
  }
}

case class CreateResponse(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

object CreateResponse {
  def unapply(buf: Buf): Option[(CreateResponse, Buf)] = {
    val BufString(path, rem) = buf
    Some(CreateResponse(path), rem)
  }
}

case class Create2Response(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

object Create2Response {
  def unapply(buf: Buf): Option[(Create2Response, Buf)] = {
    val BufString(path, rem) = buf
    Some(Create2Response(path), rem)
  }
}

case class ExistsRequest(
  path: String,
  watch: Boolean
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufBool(watch))
}

object ExistsRequest {
  def unapply(buf: Buf): Option[(ExistsRequest, Buf)] = {
    val BufString(path, BufBool(watch, rem)) = buf
    Some(ExistsRequest(path, watch), rem)
  }
}

case class ExistsResponse(
  stat: Stat
) extends Packet {
  def buf: Buf = stat.buf
}

object ExistsResponse {
  def unapply(buf: Buf): Option[(ExistsResponse, Buf)] = {
    val Stat(stat, rem) = buf
    Some(ExistsResponse(stat), rem)
  }
}

case class GetDataResponse(
  data: Array[Byte],
  stat: Stat
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufArray(data))
    .concat(stat.buf)
}

object GetDataResponse {
  def unapply(buf: Buf): Option[(GetDataResponse, Buf)] = {
    val BufArray(data, Stat(stat, rem)) = buf
    Some(GetDataResponse(data, stat), rem)
  }
}

case class GetChildrenResponse(
  children: Seq[String]
) extends Packet {
  def buf: Buf = BufSeqString(children)
}

object GetChildrenResponse {
  def unapply(buf: Buf): Option[(GetChildrenResponse, Buf)] = {
    val BufSeqString(children, rem) = buf
    Some(GetChildrenResponse(children), buf)
  }
}

case class GetChildren2Response(
  children: Seq[String],
  stat: Stat
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufSeqString(children))
    .concat(stat.buf)
}

object GetChildren2Response {
  def unapply(buf: Buf): Option[(GetChildren2Response, Buf)] = {
    val BufSeqString(children, Stat(stat, rem)) = buf
    Some(GetChildren2Response(children, stat), buf)
  }
}

case class GetACLResponse(
  acl: Seq[ACL],
  stat: Stat
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufSeqACL(acl))
    .concat(stat.buf)
}

object GetACLResponse {
  def unapply(buf: Buf): Option[(GetACLResponse, Buf)] = {
    val BufSeqACL(acl, Stat(stat, rem)) = buf
    Some(GetACLResponse(acl, stat), buf)
  }
}

case class CheckWatchesRequest(
  path: String,
  typ: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(typ))
}

object CheckWatchesRequest {
  def unapply(buf: Buf): Option[(CheckWatchesRequest, Buf)] = {
    val BufString(path, BufInt(typ, rem)) = buf
    Some(CheckWatchesRequest(path, typ), rem)
  }
}

case class RemoveWatchesRequest(
  path: String,
  typ: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(typ))
}

object RemoveWatchesRequest {
  def unapply(buf: Buf): Option[(RemoveWatchesRequest, Buf)] = {
    val BufString(path, BufInt(typ, rem)) = buf
    Some(RemoveWatchesRequest(path, typ), rem)
  }
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

object LearnerInfo {
  def unapply(buf: Buf): Option[(LearnerInfo, Buf)] = {
    val BufLong(serverid, BufInt(protocolVersion, BufLong(configVersion, rem))) = buf
    Some(LearnerInfo(serverid, protocolVersion, configVersion), rem)
  }
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
    .concat(BufSeqId(authinfo))
}

object QuorumPacket {
  def unapply(buf: Buf): Option[(QuorumPacket, Buf)] = {
    val BufInt(typ,
        BufLong(zxid,
        BufArray(data,
        BufSeqId(authinfo,
        rem
    )))) = buf
    Some(QuorumPacket(typ, zxid, data, authinfo), rem)
  }
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

object FileHeader {
  def unapply(buf: Buf): Option[(FileHeader, Buf)] = {
    val BufInt(magic, BufInt(version, BufLong(dbid, rem))) = buf
    Some(FileHeader(magic, version, dbid), rem)
  }
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

object TxnHeader {
  def unapply(buf: Buf): Option[(TxnHeader, Buf)] = {
    val BufLong(clientId,
        BufInt(cxid,
        BufLong(zxid,
        BufLong(time,
        BufInt(typ,
        rem
    ))))) = buf
    Some(TxnHeader(clientId, cxid, zxid, time, typ), rem)
  }
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
    .concat(BufSeqACL(acl))
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
    .concat(BufSeqACL(acl))
    .concat(BufBool(ephemeral))
    .concat(BufInt(parentCVersion))
}

object CreatTxn {
  def unapply(buf: Buf): Option[(CreateTxn, Buf)] = {
    val BufString(path,
        BufArray(data,
        BufSeqACL(acl,
        BufBool(ephemeral,
        BufInt(parentCVersion,
        rem
    ))))) = buf
    Some(CreateTxn(path, data, acl, ephemeral, parentCVersion), rem)
  }
}

case class DeleteTxt(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

object DeleteTxt {
  def unapply(buf: Buf): Option[(DeleteTxt, Buf)] = {
    val BufString(path, rem) = buf
    Some(DeleteTxt(path), rem)
  }
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

object SetDataTxn {
  def unapply(buf: Buf): Option[(SetDataTxn, Buf)] = {
    val BufString(path, BufArray(data, BufInt(version, rem))) = buf
    Some(SetDataTxn(path, data, version), rem)
  }
}

case class CheckVersionTxn(
  path: String,
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(version))
}

object CheckVersionTxn {
  def unapply(buf: Buf): Option[(CheckVersionTxn, Buf)] = {
    val BufString(path, BufInt(version, rem)) = buf
    Some(CheckVersionTxn(path, version), rem)
  }
}

case class SetACLTxn(
  path: String,
  acl: Seq[ACL],
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufSeqACL(acl))
    .concat(BufInt(version))
}

object SetACLTxn {
  def unapply(buf: Buf): Option[(SetACLTxn, Buf)] = {
    val BufString(path, BufSeqACL(acl, BufInt(version, rem))) = buf
    Some(SetACLTxn(path, acl, version), rem)
  }
}

case class SetMaxChildrenTxn(
  path: String,
  max: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufInt(max))
}

object SetMaxChildrenTxn {
  def unapply(buf: Buf): Option[(SetMaxChildrenTxn, Buf)] = {
    val BufString(path, BufInt(max, rem)) = buf
    Some(SetMaxChildrenTxn(path, max), rem)
  }
}

case class CreateSessionTxn(
  timeOut: Int
) extends Packet {
  def buf: Buf = BufInt(timeOut)
}

object CreateSessionTxn {
  def unapply(buf: Buf): Option[(CreateSessionTxn, Buf)] = {
    val BufInt(timeOut, rem) = buf
    Some(CreateSessionTxn(timeOut), rem)
  }
}

case class ErrorTxn(
  err: Int
) extends Packet {
  def buf: Buf = BufInt(err)
}

object ErrorTxn {
  def unapply(buf: Buf): Option[(ErrorTxn, Buf)] = {
    val BufInt(err, rem) = buf
    Some(ErrorTxn(err), rem)
  }
}

case class Txn(
  typ: Int,
  data: Array[Byte]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufInt(typ))
    .concat(BufArray(data))
}

object Txn {
  def unapply(buf: Buf): Option[(Txn, Buf)] = {
    val BufInt(typ, BufArray(data, rem)) = buf
    Some(Txn(typ, data), rem)
  }
}

case class MultiTxn(
  txns: Seq[Txn]
) extends Packet {
  def buf: Buf = BufSeqTxn(txns)
}

object MultiTxn {
  def unapply(buf: Buf): Option[(MultiTxn, Buf)] = {
    val BufSeqTxn(txns, rem) = buf
    Some(MultiTxn(txns), rem)
  }
}
