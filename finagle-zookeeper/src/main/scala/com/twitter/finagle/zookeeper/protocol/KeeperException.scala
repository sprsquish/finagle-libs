package com.twitter.finagle.zookeeper.protocol

sealed abstract class KeeperException(val code: Int) extends Exception with NoStacktrace
object KeeperException {
  object Ok extends KeeperException(0)
  object SystemError extends KeeperException(-1)
  object RuntimeInconsistency extends KeeperException(-2)
  object DataInconsistency extends KeeperException(-3)
  object ConnectionLoss extends KeeperException(-4)
  object MarshallingError extends KeeperException(-5)
  object Unimplemented extends KeeperException(-6)
  object OperationTimeout extends KeeperException(-7)
  object BadArguments extends KeeperException(-8)
  object UnknownSession extends KeeperException(-12)
  object NewConfigNoQuorum extends KeeperException(-13)
  object ReconfigInProgress extends KeeperException(-14)
  object APIError extends KeeperException(-100)
  object NoNode extends KeeperException(-101)
  object NoAuth extends KeeperException(-102)
  object BadVersion extends KeeperException(-103)
  object NoChildrenForEphemerals extends KeeperException(-108)
  object NodeExists extends KeeperException(-110)
  object NotEmpty extends KeeperException(-111)
  object SessionExpired extends KeeperException(-112)
  object InvalidCallback extends KeeperException(-113)
  object InvalidACL extends KeeperException(-114)
  object AuthFailed extends KeeperException(-115)
  object SessionMoved extends KeeperException(-118)
  object NotReadOnly extends KeeperException(-119)
  object EphemeralOnLocalSession extends KeeperException(-120)
  object NoWatcher extends KeeperException(-121)

  def apply(code: Int): KeeperException = code match {
    case 0 => Ok
    case -1 => SystemError
    case -2 => RuntimeInconsistency
    case -3 => DataInconsistency
    case -4 => ConnectionLoss
    case -5 => MarshallingError
    case -7 => OperationTimeout
    case -8 => BadArguments
    case -12 => UnknownSession
    case -13 => NewConfigNoQuorum
    case -14 => ReconfigInProgress
    case -100 => APIError
    case -101 => NoNode
    case -102 => NoAuth
    case -103 => BadVersion
    case -108 => NoChildrenForEphemerals
    case -110 => NodeExists
    case -111 => NotEmpty
    case -112 => SessionExpired
    case -113 => InvalidCallback
    case -114 => InvalidACL
    case -115 => AuthFailed
    case -118 => SessionMoved
    case -119 => NotReadOnly
    case -120 => EphemeralOnLocalSession
    case -121 => NoWatcher
  }

  def unapply(code: Int): Option[KeeperException] =
    try { Some(apply(code)) } catch { case _: Throwable => None }
}

