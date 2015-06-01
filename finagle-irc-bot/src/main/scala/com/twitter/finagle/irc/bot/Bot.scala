package com.twitter.finagle.irc.bot

object Config {
  case class Server(
    hostname: String,
    name: String,
    username: String,
    password: Option[String] = None,
    channels: Seq[Channel] = Seq.empty[Channel])

  case class Channel(
    name: String,
    password: Option[String] = None)
}

class Bot(config: Config.Server, outgoing: Broker[Message], handle: IrcHandle) {
  def onClose = handle.onClose

  val plugins = config.plugins flatMap { name =>
    Plugins(name) map { p => (p.cmd, p) }
  } toMap

  handle.message foreach {
    case _: Message => ()
  }
}
