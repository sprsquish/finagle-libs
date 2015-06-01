package com.twitter.finagle.irc.bot

object Main extends TwitterServer {
  val configFilename = flag("config.filename", "", "location of the JSON encoded config file")

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def main() {
    val serverConfigs = mapper.readValue[Seq[Config.Server]](new File(configFilename()))
    val closed = serverConfgs map { server =>
      val outgoing = new Broker[Message]
      val handle = Irc.newClient(server.hostname)(outgoing.rcv)
      (new Bot(server, outgoing, handle)).onClose
    }

    Await.ready(Future.join(closed))
  }
}
