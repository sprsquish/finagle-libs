package com.twitter.finagle.zookeeper.protocol

import com.twitter.finagle.NoStacktrace
import com.twitter.util.{Future, Promise}
import scala.collection.mutable

trait WatchManager {
  def apply(evt: WatcherEvent): Future[Unit] =
    apply(WatchedEvent(evt))

  def apply(evt: WatchedEvent): Future[Unit]

  def existsWatch(path: String): Future[WatchedEvent]
  def dataWatch(path: String): Future[WatchedEvent]
  def childrenWatch(path: String): Future[WatchedEvent]
}

class DefaultWatchManager extends WatchManager {
  private[this] val dataTable = new mutable.HashMap[String, Promise[WatchedEvent]]
  private[this] val existTable = new mutable.HashMap[String, Promise[WatchedEvent]]
  private[this] val childTable = new mutable.HashMap[String, Promise[WatchedEvent]]

  // TODO: should this be shunted off to another threadpool?
  // for now the read thread will end up satisfying all watches
  def apply(evt: WatchedEvent): Future[Unit] = {
    val watches = evt.typ match {
      case EventType.None =>
        Seq(
          watchesFor(dataTable, None),
          watchesFor(existTable, None),
          watchesFor(childTable, None)
        ).flatten

      case EventType.NodeDataChanged | EventType.NodeCreated =>
        Seq(
          watchesFor(dataTable, evt.path),
          watchesFor(existTable, evt.path)
        ).flatten

      case EventType.NodeChildrenChanged =>
        watchesFor(childTable, evt.path)

      case EventType.NodeDeleted =>
        Seq(
          watchesFor(dataTable, evt.path),
          watchesFor(existTable, evt.path),
          watchesFor(childTable, evt.path)
        ).flatten

      case _ =>
        Seq.empty[Promise[WatchedEvent]]
    }

    watches foreach { _.setValue(evt) }

    Future.Done
  }

  private[this] def watchesFor(
    table: mutable.Map[String, Promise[WatchedEvent]],
    path: Option[String]
  ): Seq[Promise[WatchedEvent]] =
    table.synchronized {
      path match {
        case Some(p) => table.remove(p).toSeq
        case None =>
          val watches = table.values.toList
          table.clear()
          watches
      }
    }

  def existsWatch(path: String): Future[WatchedEvent] =
    existTable.synchronized { existTable.getOrElseUpdate(path, new Promise[WatchedEvent]) }

  def dataWatch(path: String): Future[WatchedEvent] =
    dataTable.synchronized { dataTable.getOrElseUpdate(path, new Promise[WatchedEvent]) }

  def childrenWatch(path: String): Future[WatchedEvent] =
    childTable.synchronized { childTable.getOrElseUpdate(path, new Promise[WatchedEvent]) }
}

case class WatchedEvent(typ: EventType, state: KeeperState, path: Option[String] = None)
object WatchedEvent {
  def apply(evt: WatcherEvent): WatchedEvent =
    WatchedEvent(EventType(evt.typ), KeeperState(evt.state), Option(evt.path))
}
