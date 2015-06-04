package com.twitter.finagle.zookeeper

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.{Transport => FTransport}
import com.twitter.io.Buf
import com.twitter.util.{Await, Future, Promise, Time}
import java.net.SocketAddress
import java.util.{LinkedList, Queue}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TransportTest extends FunSuite {
  class MockTransport(
    msgs: Queue[ChannelBuffer]
  ) extends FTransport[ChannelBuffer, ChannelBuffer] {
    def isOpen = true
    val onClose = new Promise[Throwable]
    def localAddress = new SocketAddress {}
    def remoteAddress = new SocketAddress {}
    def close(deadline: Time) = Future.Done

    def write(req: ChannelBuffer): Future[Unit] = {
      msgs.offer(req)
      Future.Done
    }

    def read(): Future[ChannelBuffer] = {
      Future.value(msgs.remove())
    }
  }

  test("writes a framed message") {
    val msgs = new LinkedList[ChannelBuffer]
    val trans = Transport(new MockTransport(msgs))

    val msg = Buf.Utf8("message")
    trans.write(msg)

    assert(!msgs.isEmpty)
    val Buf.U32BE(7, Buf.Utf8("message")) =
      ChannelBufferBuf(msgs.remove())
  }

  test("reads a framed message") {
    val msgs = new LinkedList[ChannelBuffer]
    val msg = ChannelBuffers.dynamicBuffer()
    msg.writeInt(7)
    msg.writeBytes("message".getBytes("UTF-8"))
    msgs.offer(msg)

    val trans = Transport(new MockTransport(msgs))
    assert(Await.result(trans.read()) === Buf.Utf8("message"))
  }

  test("buffers incoming messages") {
    val msgs = new LinkedList[ChannelBuffer]

    val msg1 = ChannelBuffers.dynamicBuffer()
    msg1.writeInt(7)
    msg1.writeBytes("mess".getBytes("UTF-8"))
    msgs.offer(msg1)

    val msg2 = ChannelBuffers.dynamicBuffer()
    msg2.writeBytes("age".getBytes("UTF-8"))
    msgs.offer(msg2)

    val trans = Transport(new MockTransport(msgs))
    assert(Await.result(trans.read()) === Buf.Utf8("message"))
  }
}
