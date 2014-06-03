package com.twitter.finagle.zookeeper.protocol

import com.twitter.util.{Future, Promise}
import scala.collection.mutable

sealed abstract class WatchType(val value: Int)
object WatchType {
  object Children extends WatchType(1)
  object Data extends WatchType(2)
  object Any extends WatchType(3)
}

trait Watch extends Future[WatchedEvent] {
  def remove(): Future[Unit]
}
trait PendingWatch extends Promise[WatchedEvent] with Watch

trait WatchManager {
  def apply(evt: WatcherEvent): Future[Unit] =
    apply(WatchedEvent(evt))

  def apply(evt: WatchedEvent): Future[Unit]

  def existsWatch(path: String): Future[WatchedEvent]
  def dataWatch(path: String): Future[WatchedEvent]
  def childrenWatch(path: String): Future[WatchedEvent]
}

class DefaultWatchManager(
  sendRemoveWatch: (WatchType, String) => Future[Unit]
) extends WatchManager {
  private[this] val dataTable = new mutable.HashMap[String, PendingWatch]
  private[this] val existTable = new mutable.HashMap[String, PendingWatch]
  private[this] val childTable = new mutable.HashMap[String, PendingWatch]

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
        Seq.empty[PendingWatch]
    }

    watches foreach { _.setValue(evt) }

    Future.Done
  }

  private[this] def watchesFor(
    table: mutable.Map[String, PendingWatch],
    path: Option[String]
  ): Seq[PendingWatch] =
    table.synchronized {
      path match {
        case Some(p) => table.remove(p).toSeq
        case None =>
          val watches = table.values.toList
          table.clear()
          watches
      }
    }

  private[this] def removeWatch(
    table: mutable.Map[String, Set[PendingWatch]],
    path: String,
    watch: PendingWatch
  ): Future[Unit] = table.synchronized {
    table.remove(path) map { set =>
      val newSet = set - watch
      if (!newSet.isEmpty) {
        table(path) = newSet
        Future.Done
      } else {
        sendRemoveWatch(typ, path)
      }
    } getOrElse(Future.Done)
  }

  private[this] def addWatch(
    table: mutable.Map[String, Set[PendingWatch]],
    path: String
  ): Watch = table.synchronized {
    val watch = new PendingWatch { self =>
      def remove(): Future[Unit] = removeWatch(table, path, self)
    }
    watch.setInterruptHandler { case _: Throwable => watch.remove() }

    val set = table.getOrElse(path, Set.empty[PendingWatch])
    table(path) = set + watch
    watch
  }

  def existsWatch(path: String): Future[WatchedEvent] =
    existTable.synchronized { existTable.getOrElseUpdate(path, new PendingWatch) }

  def dataWatch(path: String): Future[WatchedEvent] =
    dataTable.synchronized { dataTable.getOrElseUpdate(path, new PendingWatch) }

  def childrenWatch(path: String): Future[WatchedEvent] =
    childTable.synchronized { childTable.getOrElseUpdate(path, new PendingWatch) }
}

case class WatchedEvent(typ: EventType, state: KeeperState, path: Option[String] = None)
object WatchedEvent {
  def apply(evt: WatcherEvent): WatchedEvent =
    WatchedEvent(EventType(evt.typ), KeeperState(evt.state), Option(evt.path))
}
