package edu.netty

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise, promise}
import io.netty.channel._
import edu.netty.NettyClient._
import io.netty.bootstrap.Bootstrap
import java.util.concurrent.{TimeUnit, ScheduledExecutorService}
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import edu.netty.NettyClient.StateUpdateResult
import edu.netty.NettyClient.WaitingForConnection

class NettyClient(bs: Bootstrap, scheduler: ScheduledExecutorService) {
  private val stateRef = new AtomicReference[ClientState](new WaitingForConnection(Queue.empty) with EventLogger )

  @tailrec
  private def updateState[T](f: ClientState => StateUpdateResult[T]): T = {
    val oldState = stateRef.get()
    val StateUpdateResult(res, newState) = f(oldState)
    if(stateRef.compareAndSet(oldState, newState)) {
      res
    } else {
      updateState(f)
    }
  }

  private class NettySender(channel: Channel) extends Sender {
    def send(cmd: NettyClient.CommandAndResponse) {
      channel.writeAndFlush(cmd._1).addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture) {
          if(future.isSuccess) {
            updateState(_.commandSent(cmd))
          } else {
            updateState(_.sendFailure(cmd)).apply()
            future.channel().pipeline().fireExceptionCaught(future.cause())

          }
        }
      })
    }

    def close() {
      channel.close()
    }
  }

  private class NettyConnector extends Connector {
    @tailrec
    private def failAll(cause: Throwable) {
      val (toFail, toRun) = updateState(_.connectionBroken(cause, NettyConnector.this))
      toFail.foreach(_._2.failure(cause))
      toRun.apply()
      if(!toFail.isEmpty) {
        failAll(cause)
      }
    }


    def connectAsync() {
      bs.connect("localhost", 8100).addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture) {
          if(future.isSuccess) {
            println("connected")
            val channel = future.channel()
            channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
              override def channelRead(ctx: ChannelHandlerContext, msg: Any) {
                println("Client message received " + msg)
                updateState(_.responseReceived(msg.toString)).apply()
              }

              override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
                failAll(cause)
              }
            }).addLast(new ChannelOutboundHandlerAdapter() {
              override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
                failAll(cause)
              }
            })


            val sender = new NettySender(future.channel())
            @tailrec
            def sendRetries() {
              val retries = updateState(_.connectionEstablished(sender))
              if(!retries.isEmpty) {
                retries.foreach(sender.send)
                sendRetries()
              }
            }

            sendRetries()

          } else {
            val cause = future.cause()
            println("connection failed " + cause)
            failAll(cause)
          }
        }
      })
    }

    def scheduleConnect(delayMillis: Long) {
      scheduler.schedule(new Runnable {
        def run() {
          connectAsync()
        }
      }, delayMillis, TimeUnit.MILLISECONDS)
    }
  }

  new NettyConnector().connectAsync()

  def submitCommand(cmd: String)(implicit ctx: ExecutionContext): Future[String] = {
    val responsePromise = promise[String]()
    updateState(_.send((cmd, responsePromise))).apply()
    responsePromise.future
  }


}


object NettyClient {
  type CommandAndResponse = (String, Promise[String])
  type CommandQueue = Queue[CommandAndResponse]
  case class StateUpdateResult[T](articact: T, newState: ClientState)

  type ArtifactFunction = Function0[Unit]
  object DoNothing extends ArtifactFunction {
    def apply() {}
  }

  trait Sender {
    def send(cmd: CommandAndResponse)
    def close()
  }

  trait Connector {
    def connectAsync()
    def scheduleConnect(delayMillis: Long)
  }

  trait ClientState {
    protected def illegalState(methodName: String) = throw new IllegalArgumentException("Unexpected call of " + methodName + " in " + getClass.getSimpleName)

    def send(cmd: CommandAndResponse): StateUpdateResult[ArtifactFunction]

    def commandSent(cmd: CommandAndResponse): StateUpdateResult[Unit]
    def sendFailure(cmd: CommandAndResponse): StateUpdateResult[ArtifactFunction]

    def responseReceived(response: String): StateUpdateResult[ArtifactFunction]


    def connectionEstablished(sender: Sender): StateUpdateResult[CommandQueue]
    def connectionBroken(cause: Throwable, connector: Connector): StateUpdateResult[(CommandQueue, ArtifactFunction)]
  }

  trait EventLogger extends ClientState{
    private def logCall(method: String, args: Any*) {
      println(getClass.getSuperclass.getSimpleName + "." + method + ": " + args.map(_.toString.replace("\n","\\n")))
    }

    abstract override def send(cmd: NettyClient.CommandAndResponse): StateUpdateResult[NettyClient.ArtifactFunction] = {
      logCall("send", cmd)
      super.send(cmd)

    }

    abstract override def commandSent(cmd: NettyClient.CommandAndResponse): StateUpdateResult[Unit] = {
      logCall("commandSent", cmd)
      super.commandSent(cmd)
    }

    abstract override def sendFailure(cmd: NettyClient.CommandAndResponse): StateUpdateResult[ArtifactFunction] = {
      logCall("sendFailure", cmd)
      super.sendFailure(cmd)
    }

    abstract override def responseReceived(response: String): StateUpdateResult[NettyClient.ArtifactFunction] = {
      logCall("responseReceived", response)
      super.responseReceived(response)
    }

    abstract override def connectionEstablished(sender: Sender): StateUpdateResult[NettyClient.CommandQueue] = {
      logCall("connectionEstablished", sender)
      super.connectionEstablished(sender)
    }

    abstract override def connectionBroken(cause: Throwable, connector: Connector): StateUpdateResult[(NettyClient.CommandQueue, NettyClient.ArtifactFunction)] = {
      logCall("connectionBroken", cause, connector)
      super.connectionBroken(cause,connector)
    }
  }

  case class WaitingForConnection(pending: CommandQueue) extends ClientState {
    override def send(cmd: CommandAndResponse) = StateUpdateResult(DoNothing, new WaitingForConnection(pending.enqueue(cmd)) with EventLogger)

    override def commandSent(cmd: CommandAndResponse): StateUpdateResult[Unit] = illegalState("commandSent")

    override def sendFailure(cmd: NettyClient.CommandAndResponse): StateUpdateResult[ArtifactFunction] = illegalState("sendFailure")

    override def responseReceived(response: String): StateUpdateResult[ArtifactFunction] = illegalState("responseReceived")

    override def connectionEstablished(sender: Sender) = if(pending.isEmpty) {
      StateUpdateResult(Queue.empty[CommandAndResponse], new ConnectionEstablished(sender, Queue.empty, Queue.empty) with EventLogger)
    } else {
      StateUpdateResult(pending, new WaitingForConnection(Queue.empty) with EventLogger)
    }

    override def connectionBroken(cause: Throwable, connector: Connector) = if(pending.isEmpty) {
      StateUpdateResult((Queue.empty, () => connector.scheduleConnect(5000)), new ConnectionBroken(cause) with EventLogger)
    } else {
      StateUpdateResult((pending, DoNothing), new WaitingForConnection(Queue.empty) with EventLogger)
    }
  }




  case class ConnectionEstablished(sender: Sender, pendingReplies: CommandQueue, retries: CommandQueue) extends ClientState {
    def send(cmd: CommandAndResponse) = StateUpdateResult(() => sender.send(cmd), this)

    def commandSent(cmd: CommandAndResponse) = StateUpdateResult((), new ConnectionEstablished(sender, pendingReplies.enqueue(cmd), retries) with EventLogger)

    def sendFailure(cmd: NettyClient.CommandAndResponse) = StateUpdateResult(() => sender.close(), new ConnectionEstablished(sender, pendingReplies, retries.enqueue(cmd)) with EventLogger)

    def responseReceived(response: String) = {
      val (hd, tl) = pendingReplies.dequeue
      StateUpdateResult(() => hd._2.success(response), new ConnectionEstablished(sender, tl, retries) with EventLogger)
    }

    def connectionEstablished(sender: Sender): StateUpdateResult[CommandQueue] = illegalState("connectionEstablished")

    def connectionBroken(cause: Throwable, connector: Connector) = if(pendingReplies.isEmpty) {
      StateUpdateResult((Queue.empty, () => connector.connectAsync()), new WaitingForConnection(retries) with EventLogger)
    } else {
      StateUpdateResult((pendingReplies, DoNothing), new ConnectionEstablished(sender, Queue.empty, retries) with EventLogger)
    }


  }

  case class ConnectionBroken(cause: Throwable) extends ClientState {
    def send(cmd: NettyClient.CommandAndResponse) = StateUpdateResult(() => cmd._2.failure(cause), this)

    def commandSent(cmd: NettyClient.CommandAndResponse): StateUpdateResult[Unit] = illegalState("commandSent")

    def sendFailure(cmd: NettyClient.CommandAndResponse): StateUpdateResult[ArtifactFunction] = illegalState("sendFailure")

    def responseReceived(response: String): StateUpdateResult[NettyClient.ArtifactFunction] = illegalState("responseReceived")

    def connectionEstablished(sender: Sender): StateUpdateResult[NettyClient.CommandQueue] = StateUpdateResult(Queue.empty, ConnectionEstablished(sender, Queue.empty, Queue.empty))

    def connectionBroken(cause: Throwable, connector: Connector): StateUpdateResult[(NettyClient.CommandQueue, NettyClient.ArtifactFunction)] =
      StateUpdateResult((Queue.empty, () => connector.scheduleConnect(5000)), this)
  }


}




