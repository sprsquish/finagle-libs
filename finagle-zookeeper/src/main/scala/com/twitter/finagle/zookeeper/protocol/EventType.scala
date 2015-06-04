package com.twitter.finagle.zookeeper.protocol

sealed abstract class EventType(val code: Int)
object EventType {
  object None extends EventType(-1)
  object NodeCreated extends EventType(1)
  object NodeDeleted extends EventType(2)
  object NodeDataChanged extends EventType(3)
  object NodeChildrenChanged extends EventType(4)
  object DataWatchRemoved extends EventType(5)
  object ChildWatchRemoved extends EventType(6)

  def apply(code: Int) = code match {
    case -1 => None
    case 1 => NodeCreated
    case 2 => NodeDeleted
    case 3 => NodeDataChanged
    case 4 => NodeChildrenChanged
    case 5 => DataWatchRemoved
    case 6 => ChildWatchRemoved
  }
}
