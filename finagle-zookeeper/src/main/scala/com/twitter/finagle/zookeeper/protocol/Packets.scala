package com.twitter.finagle.zookeeper.protocol

import com.twitter.io.Buf

sealed trait Packet {
  def buf: Buf
}

sealed trait RequestPacket[Response <: Packet] extends Packet {
  val opCode: Int
  val decodeResponse: Buf => Option[(Response, Buf)]
}

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
    .concat(Buf.U32BE(perms))
    .concat(id.buf)
}

object ACL {
  def unapply(buf: Buf): Option[(ACL, Buf)] = {
    val Buf.U32BE(perms, Id(id, rem)) = buf
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
    .concat(Buf.U64BE(czxid))
    .concat(Buf.U64BE(mzxid))
    .concat(Buf.U64BE(ctime))
    .concat(Buf.U64BE(mtime))
    .concat(Buf.U32BE(version))
    .concat(Buf.U32BE(cversion))
    .concat(Buf.U32BE(aversion))
    .concat(Buf.U64BE(ephemeralOwner))
    .concat(Buf.U32BE(dataLength))
    .concat(Buf.U32BE(numChildren))
    .concat(Buf.U64BE(pzxid))
}

object Stat {
  def unapply(buf: Buf): Option[(Stat, Buf)] = {
    val Buf.U64BE(czxid,
        Buf.U64BE(mzxid,
        Buf.U64BE(ctime,
        Buf.U64BE(mtime,
        Buf.U32BE(version,
        Buf.U32BE(cversion,
        Buf.U32BE(aversion,
        Buf.U64BE(ephemeralOwner,
        Buf.U32BE(dataLength,
        Buf.U32BE(numChildren,
        Buf.U64BE(pzxid,
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
    .concat(Buf.U64BE(czxid))
    .concat(Buf.U64BE(mzxid))
    .concat(Buf.U64BE(ctime))
    .concat(Buf.U64BE(mtime))
    .concat(Buf.U32BE(version))
    .concat(Buf.U32BE(cversion))
    .concat(Buf.U32BE(aversion))
    .concat(Buf.U64BE(ephemeralOwner))
    .concat(Buf.U64BE(pzxid))
}

object StatPersisted {
  def unapply(buf: Buf): Option[(StatPersisted, Buf)] = {
    val Buf.U64BE(czxid,
        Buf.U64BE(mzxid,
        Buf.U64BE(ctime,
        Buf.U64BE(mtime,
        Buf.U32BE(version,
        Buf.U32BE(cversion,
        Buf.U32BE(aversion,
        Buf.U64BE(ephemeralOwner,
        Buf.U64BE(pzxid,
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
    .concat(Buf.U32BE(protocolVersion))
    .concat(Buf.U64BE(lastZxidSeen))
    .concat(Buf.U32BE(timeOut))
    .concat(Buf.U64BE(sessionId))
    .concat(BufArray(passwd))
}

object ConnectRequest {
  def unapply(buf: Buf): Option[(ConnectRequest, Buf)] = {
    val Buf.U32BE(protocolVersion,
        Buf.U64BE(lastZxidSeen,
        Buf.U32BE(timeOut,
        Buf.U64BE(sessionId,
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
    .concat(Buf.U32BE(protocolVersion))
    .concat(Buf.U32BE(timeOut))
    .concat(Buf.U64BE(sessionId))
    .concat(BufArray(passwd))
}

object ConnectResponse {
  def unapply(buf: Buf): Option[(ConnectResponse, Buf)] = {
    val Buf.U32BE(protocolVersion,
        Buf.U32BE(timeOut,
        Buf.U64BE(sessionId,
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
    .concat(Buf.U64BE(relativeZxid))
    .concat(BufSeqString(dataWatches))
    .concat(BufSeqString(existWatches))
    .concat(BufSeqString(childWatches))
}

object SetWatches {
  def unapply(buf: Buf): Option[(SetWatches, Buf)] = {
    val Buf.U64BE(relativeZxid,
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
    .concat(Buf.U32BE(xid))
    .concat(Buf.U32BE(typ))
}

object RequestHeader {
  def unapply(buf: Buf): Option[(RequestHeader, Buf)] = {
    val Buf.U32BE(xid, Buf.U32BE(typ, rem)) = buf
    Some(RequestHeader(xid, typ), rem)
  }
}

case class MultiRequest(
  ops: Seq[Multi.Op]
) extends RequestPacket[MultiResponse] {
  val opCode = OpCodes.Multi
  val decodeResponse = MultiResponse.unapply(_: Buf)
  def buf: Buf = Multi.encode(ops)
}

case class MultiResponse(
  results: Seq[Multi.OpResult]
) extends Packet {
  def buf: Buf = Buf.Empty
}

object MultiResponse {
  def unapply(buf: Buf): Option[(MultiResponse, Buf)] = {
    val (res, rem) = Multi.decode(buf)
    Some(MultiResponse(res), rem)
  }
}

case class MultiHeader(
  typ: Int,
  done: Boolean,
  err: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(Buf.U32BE(typ))
    .concat(BufBool(done))
    .concat(Buf.U32BE(err))
}

object MultiHeader {
  def unapply(buf: Buf): Option[(MultiHeader, Buf)] = {
    val Buf.U32BE(typ, BufBool(done, Buf.U32BE(err, rem))) = buf
    Some(MultiHeader(typ, done, err), rem)
  }
}

case class AuthPacket(
  typ: Int,
  scheme: String,
  auth: Array[Byte]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(Buf.U32BE(typ))
    .concat(BufString(scheme))
    .concat(BufArray(auth))
}

object AuthPacket {
  def unapply(buf: Buf): Option[(AuthPacket, Buf)] = {
    val Buf.U32BE(typ, BufString(scheme, BufArray(auth, rem))) = buf
    Some(AuthPacket(typ, scheme, auth), rem)
  }
}

case class ReplyHeader(
  xid: Int,
  zxid: Long,
  err: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(Buf.U32BE(xid))
    .concat(Buf.U64BE(zxid))
    .concat(Buf.U32BE(err))
}

object ReplyHeader {
  def unapply(buf: Buf): Option[(ReplyHeader, Buf)] = {
    val Buf.U32BE(xid, Buf.U64BE(zxid, Buf.U32BE(err, rem))) = buf
    Some(ReplyHeader(xid, zxid, err), rem)
  }
}

case class GetDataRequest(
  path: String,
  watch: Boolean
) extends RequestPacket[GetDataResponse] {
  val opCode = OpCodes.GetData
  val decodeResponse = GetDataResponse.unapply(_: Buf)
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
) extends RequestPacket[SetDataResponse] {
  val opCode = OpCodes.SetData
  val decodeResponse = SetDataResponse.unapply(_: Buf)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(Buf.U32BE(version))
}

object SetDataRequest {
  def unapply(buf: Buf): Option[(SetDataRequest, Buf)] = {
    val BufString(path, BufArray(data, Buf.U32BE(version, rem))) = buf
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
    .concat(Buf.U64BE(curConfigId))
}

object ReconfigRequest {
  def unapply(buf: Buf): Option[(ReconfigRequest, Buf)] = {
    val BufString(joiningServers,
        BufString(leavingServers,
        BufString(newMembers,
        Buf.U64BE(curConfigId,
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
) extends RequestPacket[Create2Response] {
  val opCode = OpCodes.Create2
  val decodeResponse = Create2Response.unapply(_: Buf)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufArray(data))
    .concat(BufSeqACL(acl))
    .concat(Buf.U32BE(flags))
}

object CreateRequest {
  def unapply(buf: Buf): Option[(CreateRequest, Buf)] = {
    val BufString(path,
        BufArray(data,
        BufSeqACL(acl,
        Buf.U32BE(flags,
        rem
    )))) = buf
    Some(CreateRequest(path, data, acl, flags), rem)
  }
}

case class DeleteRequest(
  path: String,
  version: Int
) extends RequestPacket[DeleteResponse] {
  val opCode = OpCodes.Delete
  val decodeResponse = DeleteResponse.unapply(_: Buf)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(Buf.U32BE(version))
}

object DeleteRequest {
  def unapply(buf: Buf): Option[(DeleteRequest, Buf)] = {
    val BufString(path, Buf.U32BE(version, rem)) = buf
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
) extends RequestPacket[GetChildren2Response] {
  val opCode = OpCodes.GetChildren2
  val decodeResponse = GetChildren2Response.unapply(_: Buf)
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
    .concat(Buf.U32BE(version))
}

object CheckVersionRequest {
  def unapply(buf: Buf): Option[(CheckVersionRequest, Buf)] = {
    val BufString(path, Buf.U32BE(watch, rem)) = buf
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
  def buf: Buf = Buf.U32BE(max)
}

object GetMaxChildrenResponse {
  def unapply(buf: Buf): Option[(GetMaxChildrenResponse, Buf)] = {
    val Buf.U32BE(max, rem) = buf
    Some(GetMaxChildrenResponse(max), rem)
  }
}

case class SetMaxChildrenRequest(
  path: String,
  max: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(Buf.U32BE(max))
}

object SetMaxChildrenRequest {
  def unapply(buf: Buf): Option[(SetMaxChildrenRequest, Buf)] = {
    val BufString(path, Buf.U32BE(max, rem)) = buf
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
) extends RequestPacket[GetACLResponse] {
  val opCode = OpCodes.GetACL
  val decodeResponse = GetACLResponse.unapply(_: Buf)
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
) extends RequestPacket[SetACLResponse] {
  val opCode = OpCodes.SetACL
  val decodeResponse = SetACLResponse.unapply(_: Buf)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(BufSeqACL(acl))
    .concat(Buf.U32BE(version))
}

object SetACLRequest {
  def unapply(buf: Buf): Option[(SetACLRequest, Buf)] = {
    val BufString(path, BufSeqACL(acl, Buf.U32BE(version, rem))) = buf
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
    .concat(Buf.U32BE(typ))
    .concat(Buf.U32BE(state))
    .concat(BufString(path))
}

object WatcherEvent {
  def unapply(buf: Buf): Option[(WatcherEvent, Buf)] = {
    val Buf.U32BE(typ, Buf.U32BE(state, BufString(path, rem))) = buf
    Some(WatcherEvent(typ, state, path), rem)
  }
}

case class ErrorResponse(
  err: Int
) extends Packet {
  def buf: Buf = Buf.U32BE(err)
}

object ErrorResponse {
  def unapply(buf: Buf): Option[(ErrorResponse, Buf)] = {
    val Buf.U32BE(err, rem) = buf
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
  path: String,
  stat: Stat
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(stat.buf)
}

object Create2Response {
  def unapply(buf: Buf): Option[(Create2Response, Buf)] = {
    val BufString(path, Stat(stat, rem)) = buf
    Some(Create2Response(path, stat), rem)
  }
}

case class ExistsRequest(
  path: String,
  watch: Boolean
) extends RequestPacket[ExistsResponse] {
  val opCode = OpCodes.Exists
  val decodeResponse = ExistsResponse.unapply(_: Buf)
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
) extends RequestPacket[CheckWatchesResponse] {
  val opCode = OpCodes.CheckWatches
  val decodeResponse = CheckWatchesResponse.unapply(_: Buf)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(Buf.U32BE(typ))
}

object CheckWatchesRequest {
  def unapply(buf: Buf): Option[(CheckWatchesRequest, Buf)] = {
    val BufString(path, Buf.U32BE(typ, rem)) = buf
    Some(CheckWatchesRequest(path, typ), rem)
  }
}

// a fake response because finagle requires one
case class CheckWatchesResponse() extends Packet {
  def buf: Buf = Buf.Empty
}

object CheckWatchesResponse {
  def unapply(buf: Buf): Option[(CheckWatchesResponse, Buf)] =
    Some(CheckWatchesResponse(), buf)
}

case class RemoveWatchesRequest(
  path: String,
  typ: Int
) extends RequestPacket[RemoveWatchesResponse] {
  val opCode = OpCodes.RemoveWatches
  val decodeResponse = RemoveWatchesResponse.unapply(_: Buf)
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(Buf.U32BE(typ))
}

object RemoveWatchesRequest {
  def unapply(buf: Buf): Option[(RemoveWatchesRequest, Buf)] = {
    val BufString(path, Buf.U32BE(typ, rem)) = buf
    Some(RemoveWatchesRequest(path, typ), rem)
  }
}

// a fake response because finagle requires one
case class RemoveWatchesResponse() extends Packet {
  def buf: Buf = Buf.Empty
}

object RemoveWatchesResponse {
  def unapply(buf: Buf): Option[(RemoveWatchesResponse, Buf)] =
    Some(RemoveWatchesResponse(), buf)
}

case class LearnerInfo(
  serverid: Long,
  protocolVersion: Int,
  configVersion: Long
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(Buf.U64BE(serverid))
    .concat(Buf.U32BE(protocolVersion))
    .concat(Buf.U64BE(configVersion))
}

object LearnerInfo {
  def unapply(buf: Buf): Option[(LearnerInfo, Buf)] = {
    val Buf.U64BE(serverid, Buf.U32BE(protocolVersion, Buf.U64BE(configVersion, rem))) = buf
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
    .concat(Buf.U32BE(typ))
    .concat(Buf.U64BE(zxid))
    .concat(BufArray(data))
    .concat(BufSeqId(authinfo))
}

object QuorumPacket {
  def unapply(buf: Buf): Option[(QuorumPacket, Buf)] = {
    val Buf.U32BE(typ,
        Buf.U64BE(zxid,
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
    .concat(Buf.U32BE(magic))
    .concat(Buf.U32BE(version))
    .concat(Buf.U64BE(dbid))
}

object FileHeader {
  def unapply(buf: Buf): Option[(FileHeader, Buf)] = {
    val Buf.U32BE(magic, Buf.U32BE(version, Buf.U64BE(dbid, rem))) = buf
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
    .concat(Buf.U64BE(clientId))
    .concat(Buf.U32BE(cxid))
    .concat(Buf.U64BE(zxid))
    .concat(Buf.U64BE(time))
    .concat(Buf.U32BE(typ))
}

object TxnHeader {
  def unapply(buf: Buf): Option[(TxnHeader, Buf)] = {
    val Buf.U64BE(clientId,
        Buf.U32BE(cxid,
        Buf.U64BE(zxid,
        Buf.U64BE(time,
        Buf.U32BE(typ,
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
    .concat(Buf.U32BE(parentCVersion))
}

object CreatTxn {
  def unapply(buf: Buf): Option[(CreateTxn, Buf)] = {
    val BufString(path,
        BufArray(data,
        BufSeqACL(acl,
        BufBool(ephemeral,
        Buf.U32BE(parentCVersion,
        rem
    ))))) = buf
    Some(CreateTxn(path, data, acl, ephemeral, parentCVersion), rem)
  }
}

case class DeleteTxn(
  path: String
) extends Packet {
  def buf: Buf = BufString(path)
}

object DeleteTxn {
  def unapply(buf: Buf): Option[(DeleteTxn, Buf)] = {
    val BufString(path, rem) = buf
    Some(DeleteTxn(path), rem)
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
    .concat(Buf.U32BE(version))
}

object SetDataTxn {
  def unapply(buf: Buf): Option[(SetDataTxn, Buf)] = {
    val BufString(path, BufArray(data, Buf.U32BE(version, rem))) = buf
    Some(SetDataTxn(path, data, version), rem)
  }
}

case class CheckVersionTxn(
  path: String,
  version: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(Buf.U32BE(version))
}

object CheckVersionTxn {
  def unapply(buf: Buf): Option[(CheckVersionTxn, Buf)] = {
    val BufString(path, Buf.U32BE(version, rem)) = buf
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
    .concat(Buf.U32BE(version))
}

object SetACLTxn {
  def unapply(buf: Buf): Option[(SetACLTxn, Buf)] = {
    val BufString(path, BufSeqACL(acl, Buf.U32BE(version, rem))) = buf
    Some(SetACLTxn(path, acl, version), rem)
  }
}

case class SetMaxChildrenTxn(
  path: String,
  max: Int
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(BufString(path))
    .concat(Buf.U32BE(max))
}

object SetMaxChildrenTxn {
  def unapply(buf: Buf): Option[(SetMaxChildrenTxn, Buf)] = {
    val BufString(path, Buf.U32BE(max, rem)) = buf
    Some(SetMaxChildrenTxn(path, max), rem)
  }
}

case class CreateSessionTxn(
  timeOut: Int
) extends Packet {
  def buf: Buf = Buf.U32BE(timeOut)
}

object CreateSessionTxn {
  def unapply(buf: Buf): Option[(CreateSessionTxn, Buf)] = {
    val Buf.U32BE(timeOut, rem) = buf
    Some(CreateSessionTxn(timeOut), rem)
  }
}

case class ErrorTxn(
  err: Int
) extends Packet {
  def buf: Buf = Buf.U32BE(err)
}

object ErrorTxn {
  def unapply(buf: Buf): Option[(ErrorTxn, Buf)] = {
    val Buf.U32BE(err, rem) = buf
    Some(ErrorTxn(err), rem)
  }
}

case class Txn(
  typ: Int,
  data: Array[Byte]
) extends Packet {
  def buf: Buf = Buf.Empty
    .concat(Buf.U32BE(typ))
    .concat(BufArray(data))
}

object Txn {
  def unapply(buf: Buf): Option[(Txn, Buf)] = {
    val Buf.U32BE(typ, BufArray(data, rem)) = buf
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
