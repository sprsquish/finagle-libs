package com.twitter.finagle

import com.twitter.finagle.zookeeper._
import com.twitter.finagle.client._
import com.twitter.finagle.netty3._
import com.twitter.finagle.server._
import com.twitter.finagle.transport.{Transport => FTransport}
import com.twitter.io.Buf
import com.twitter.util.Future
import java.net.SocketAddress
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.buffer.ChannelBuffer

object PipelineFactory extends ChannelPipelineFactory {
  def getPipeline = Channels.pipeline()
}

object NettyTrans extends Netty3Transporter[ChannelBuffer, ChannelBuffer](
  "zookeeper", PipelineFactory)

object ZooKeeperClient extends DefaultClient[ZkRequest, ZkResponse](
  name = "zookeeper",
  endpointer = Bridge[Buf, Buf,  ZkRequest, ZkResponse](
    NettyTrans(_, _) map { Transport(_) }, new ClientDispatcher(_)))

object ZooKeeper extends Client[ZkRequest, ZkResponse] {
  def newClient(name: Name, label: String): ServiceFactory[ZkRequest, ZkResponse] =
    ZooKeeperClient.newClient(name, label)

  def newRichClient(dest: String): ZkClient =
    new ZkClient(newClient(dest))
}
