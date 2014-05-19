package com.twitter.finagle.zookeeper

private[finagle] class ClientDispatcher(
  trans: Transport[Request, Response]
) extends Service[
