package edu.netty

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelInboundHandlerAdapter, ChannelHandlerContext, SimpleChannelInboundHandler, ChannelInitializer}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.DelimiterBasedFrameDecoder
import io.netty.buffer.Unpooled
import java.nio.charset.Charset
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import io.netty.handler.codec.string.{StringEncoder, StringDecoder}

object NettyServer extends App {
  val b = new ServerBootstrap()
  val group = new NioEventLoopGroup(1)

  try {
    val nMsgs = new AtomicInteger()

    b.group(group,group)
      .channel(classOf[NioServerSocketChannel])
      .localAddress("localhost", 8100)
      .childHandler(new ChannelInitializer[SocketChannel] {
      def initChannel(ch: SocketChannel) {
        println("connected")
        ch.pipeline()
          .addLast(new DelimiterBasedFrameDecoder(10000, Unpooled.copiedBuffer("\n",Charset.forName("UTF-8"))))
          .addLast(new StringDecoder())
          .addLast(new StringEncoder())
          .addLast(new ChannelInboundHandlerAdapter() {


          override def channelRead(ctx: ChannelHandlerContext, msg: Any) {
            println(ctx.channel() + ": server received " + msg)
            nMsgs.incrementAndGet()
            if(nMsgs.get() == 5)      {
              println("disconnecting")
              //ctx.close().sync()
              ctx.channel().close().sync()
              println("disconnected")
            }
            else
              ctx.writeAndFlush(msg + "\n")
          }

          override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            println(cause)
            ctx.close()
          }
        })
      }
    })

    val f = b.bind().sync()
    f.channel().closeFuture().sync()

  } finally {
    group.shutdownGracefully()
  }


}
