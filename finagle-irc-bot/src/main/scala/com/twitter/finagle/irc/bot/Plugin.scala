package com.twitter.finagle.irc.bot

trait Plugin {
  def name: String
  def desc: String
  def cmd: String

  def handleMsg(msg: Message): Unit
  def handleCmd(cmd
}

object Plugins {
  private[this] val plugins = LoadService[Plugin]() map { plugin =>
    (plugin.name -> plugin)
  } toMap

  def apply(name: String): Plugin = plugins.get(name)
}
