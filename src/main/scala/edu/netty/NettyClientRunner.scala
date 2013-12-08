package edu.netty

import io.netty.bootstrap.Bootstrap
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.{ChannelOption, ChannelHandlerContext, ChannelInitializer}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.{MessageToByteEncoder, ByteToMessageDecoder, DelimiterBasedFrameDecoder}
import io.netty.buffer.{ByteBuf, Unpooled}
import java.nio.charset.Charset
import java.util
import java.util.concurrent.{TimeUnit, Executors}
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration.Duration
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.codec.string.{StringEncoder, StringDecoder}
import io.netty.handler.timeout.ReadTimeoutHandler

object NettyClientRunner extends App {
  val group = new NioEventLoopGroup(1)
  val bs = new Bootstrap()
    .channel(classOf[NioSocketChannel])
    .group(group)
    .handler(new ChannelInitializer[SocketChannel] {
    def initChannel(ch: SocketChannel) {
      ch.pipeline()
//        .addLast(new ReadTimeoutHandler(1))
        .addLast(new DelimiterBasedFrameDecoder(10000, Unpooled.copiedBuffer("\n",Charset.forName("UTF-8"))))
        .addLast(new StringDecoder())
        .addLast(new StringEncoder())
    }
  })

  val scheduler = Executors.newScheduledThreadPool(1)

  val client = new NettyClient(bs, scheduler)
  implicit val ctx = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))

  scheduler.scheduleAtFixedRate(new Runnable {
    def run() {
      client.submitCommand("ping")
    }
  }, 500, 500, TimeUnit.MILLISECONDS)

  val futs = for(i <- 1 to 10)
  yield {
//    Thread.sleep(1000)
    val fut = client.submitCommand("message"  + i + "\n")
    fut.onSuccess {
      case v: String => println("received " + v)
    }
    fut.onFailure {
      case e: Throwable => println("failed processing message" + i)
    }
    fut
  }


  Await.result(Future.sequence(futs), Duration(1000, TimeUnit.SECONDS))






}
