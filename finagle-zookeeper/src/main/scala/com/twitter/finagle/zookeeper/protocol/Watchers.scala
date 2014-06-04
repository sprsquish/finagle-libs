package com.twitter.finagle.zookeeper.protocol

import com.twitter.util.{Future, Promise}
import scala.collection.mutable

sealed abstract class WatchType(val value: Int)
object WatchType {
  object Children extends WatchType(1)
  object Data extends WatchType(2)
  object Any extends WatchType(3)
}

/**
 * A Watch is a specialized Future[WatchedEvent]. It augments the
 * Future interface with a `cancel` method to cancel the watch and
 * a `watchType` which defines what this watch is listening for.
 */
trait Watch extends Future[WatchedEvent] {
  val watchType: WatchType

  /**
   * Cancel the watcher. This will not tell the server to cancel
   * the watch but will ensure this watch is never fired.
   *
   * @param local whether watch can be removed locally when there is no server connection
   */
  def cancel(local: Boolean): Future[Unit]
}

class WatchManager(
  checkWatch: (WatchType, String) => Future[Unit],
  chroot: Option[String] = None
) {
  private trait PendingWatch extends Promise[WatchedEvent] with Watch

  private[this] val dataTable = new mutable.HashMap[String, Set[PendingWatch]]
  private[this] val childTable = new mutable.HashMap[String, Set[PendingWatch]]

  def apply(evt: WatcherEvent): Future[Unit] =
    apply(WatchedEvent(evt))

  // TODO: should this be shunted off to another threadpool?
  // for now the read thread will end up satisfying all watches
  def apply(evt: WatchedEvent): Future[Unit] = {
    val watches = evt.typ match {
      case EventType.None =>
        watchesFor(dataTable, None) ++
        watchesFor(childTable, None)

      case EventType.NodeDataChanged | EventType.NodeCreated =>
        watchesFor(dataTable, evt.path)

      case EventType.NodeChildrenChanged =>
        watchesFor(childTable, evt.path)

      case EventType.NodeDeleted =>
        watchesFor(dataTable, evt.path) ++
        watchesFor(childTable, evt.path)

      case _ =>
        Set.empty[PendingWatch]
    }

    val event = chroot flatMap { c =>
      evt.path map { path =>
        evt.copy(path = Some(path.substring(c.length)))
      }
    } getOrElse(evt)

    watches foreach { _.setValue(event) }

    Future.Done
  }

  private[this] def watchesFor(
    table: mutable.Map[String, Set[PendingWatch]],
    path: Option[String]
  ): Set[PendingWatch] = table.synchronized {
    path match {
      case Some(p) => table.remove(p).getOrElse(Set.empty[PendingWatch])
      case None =>
        val watches = table.values.foldLeft(Set.empty[PendingWatch])(_ ++ _)
        table.clear()
        watches
    }
  }

  private[this] def getTable(typ: WatchType): mutable.Map[String, Set[PendingWatch]] =
    typ match {
      case WatchType.Data => dataTable
      case WatchType.Children => childTable
      case WatchType.Any => throw new Exception("no table for WatchType.Any")
    }

  private[this] def removeWatch(
    path: String,
    watch: PendingWatch,
    local: Boolean
  ): Future[Unit] = {
    val table = getTable(watch.watchType)
    def doRemove(): Unit = table.synchronized {
      table.remove(path) map { set =>
        val newSet = set - watch
        if (!newSet.isEmpty) table(path) = newSet
      }
    }

    checkWatch(watch.watchType, path) onSuccess { _ =>
      doRemove()
    } onFailure { _ =>
      if (local) doRemove()
    }
  }

  def addWatch(typ: WatchType, path: String): Watch = {
    val table = getTable(typ)

    val watch = new PendingWatch {
      def cancel(local: Boolean): Future[Unit] =
        removeWatch(path, this, local)
      val watchType = typ
    }
    watch.setInterruptHandler { case _: Throwable => watch.cancel(true) }

    table.synchronized {
      val set = table.getOrElse(path, Set.empty[PendingWatch])
      table(path) = set + watch
    }
    watch
  }
}

case class WatchedEvent(typ: EventType, state: KeeperState, path: Option[String] = None)
object WatchedEvent {
  def apply(evt: WatcherEvent): WatchedEvent =
    WatchedEvent(EventType(evt.typ), KeeperState(evt.state), Option(evt.path))
}
