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
  private val stateRef = new AtomicReference[ClientState](new WaitingForConnection(Queue.empty) with EventLogger)

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
            updateState(_.sendFailure(future.cause(), cmd)).apply()
//            future.channel().pipeline().fireExceptionCaught(future.cause())

          }
        }
      })
    }

    def close() {
      channel.close()
    }
  }

  private class NettyConnector extends Connector {

    def connectAsync() {
      bs.connect("localhost", 8100).addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture) {
          if(future.isSuccess) {
            val channel = future.channel()
            channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
              override def channelRead(ctx: ChannelHandlerContext, msg: Any) {
                updateState(_.responseReceived(msg.toString)).apply()
              }

              override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
                updateState(_.receiveFailure(cause)).apply()
              }
            })

            val sender = new NettySender(future.channel())

            updateState(_.connectionEstablished(sender, NettyConnector.this)).apply()

          } else {
            val cause = future.cause()
            println("connection failed " + cause)
            updateState(_.connectionBroken(future.cause(), NettyConnector.this)).apply()
            //failAll(cause)
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

    def commandSent(cmd: CommandAndResponse): StateUpdateResult[ArtifactFunction] = illegalState("commandSent")
    def sendFailure(cause: Throwable, cmd: CommandAndResponse): StateUpdateResult[ArtifactFunction] = illegalState("sendFailure")

    def responseReceived(response: String): StateUpdateResult[ArtifactFunction] = illegalState("responseReceived")
    def receiveFailure(cause: Throwable): StateUpdateResult[ArtifactFunction] = illegalState("receiveFailure")

    def connectionEstablished(sender: Sender, connector: Connector): StateUpdateResult[ArtifactFunction] = illegalState("connectionEstablished")
    def connectionBroken(cause: Throwable, connector: Connector): StateUpdateResult[ArtifactFunction] = illegalState("connectionBroken")
  }

  trait EventLogger extends ClientState{
    private def logCall[T](actualCall: => T, method: String, args: Any*): T = {
      println(getClass.getSuperclass.getSimpleName + "." + method + ": " + args.map(_.toString.replace("\n","\\n")))
      actualCall
    }


    abstract override def send(cmd: NettyClient.CommandAndResponse): StateUpdateResult[NettyClient.ArtifactFunction] =
      logCall(super.send(cmd), "send", cmd)

    abstract override def commandSent(cmd: NettyClient.CommandAndResponse): StateUpdateResult[ArtifactFunction] =
      logCall(super.commandSent(cmd), "commandSent", cmd)

    abstract override def sendFailure(cause: Throwable, cmd: NettyClient.CommandAndResponse): StateUpdateResult[NettyClient.ArtifactFunction] =
      logCall(super.sendFailure(cause, cmd), "sendFailure", cause, cmd)

    abstract override def responseReceived(response: String): StateUpdateResult[NettyClient.ArtifactFunction] =
      logCall(super.responseReceived(response), "responseReceived", response)

    abstract override def receiveFailure(cause: Throwable): StateUpdateResult[NettyClient.ArtifactFunction] =
      logCall(super.receiveFailure(cause), "receiveFailure", cause)

    abstract override def connectionEstablished(sender: Sender, connector: Connector): StateUpdateResult[ArtifactFunction] =
      logCall(super.connectionEstablished(sender, connector), "connectionEstablished")

    abstract override def connectionBroken(cause: Throwable, connector: Connector): StateUpdateResult[NettyClient.ArtifactFunction] =
      logCall(super.connectionBroken(cause, connector), "connectionBroken", cause)
  }

  case class WaitingForConnection(retries: CommandQueue) extends ClientState {
    override def send(cmd: CommandAndResponse) = StateUpdateResult(DoNothing, new WaitingForConnection(retries.enqueue(cmd)) with EventLogger)

    override def connectionEstablished(sender: Sender, connector: Connector) = if(retries.isEmpty) {
      StateUpdateResult(DoNothing, new ConnectionEstablished(sender, connector, 0, Queue.empty, Queue.empty) with EventLogger)
    } else {
      StateUpdateResult(() => retries.foreach(sender.send), new RetryingFailedSends(sender,connector, retries.size, Queue.empty, Queue.empty, Queue.empty) with EventLogger)
    }

    override def connectionBroken(cause: Throwable, connector: Connector) = {
      def failPendingAndScheduleReconnect() {
        retries.foreach(_._2.failure(cause))
        connector.scheduleConnect(5000)
      }

      StateUpdateResult(() => failPendingAndScheduleReconnect(), new ConnectionBroken(cause) with EventLogger)
    }
  }

  case class RetryingFailedSends(sender: Sender, connector: Connector,
                                 nPendingSends: Int,
                                 pendingReplies: CommandQueue,
                                 failedSends: CommandQueue,
                                 incomingCommands: CommandQueue) extends ClientState {

    def send(cmd: NettyClient.CommandAndResponse): StateUpdateResult[NettyClient.ArtifactFunction] =
      StateUpdateResult(DoNothing, new RetryingFailedSends(sender, connector, nPendingSends, pendingReplies, incomingCommands.enqueue(cmd), failedSends) with EventLogger)

    override def commandSent(cmd: CommandAndResponse): StateUpdateResult[ArtifactFunction] = if(nPendingSends == 1) {
      StateUpdateResult(() => incomingCommands.foreach(sender.send), new ConnectionEstablished(sender, connector, incomingCommands.size, pendingReplies.enqueue(cmd), Queue.empty) with EventLogger)
    } else {
      StateUpdateResult(DoNothing, new RetryingFailedSends(sender, connector, nPendingSends - 1, pendingReplies.enqueue(cmd), failedSends, incomingCommands) with EventLogger)
    }

    private def failPendingAndReconnect(cause: Throwable) {
      pendingReplies.foreach(_._2.failure(cause))
      sender.close()
      connector.connectAsync()
    }


    override def sendFailure(cause: Throwable, cmd: NettyClient.CommandAndResponse): StateUpdateResult[NettyClient.ArtifactFunction] = if(nPendingSends == 1) {
      StateUpdateResult(() => failPendingAndReconnect(cause), WaitingForConnection(failedSends.enqueue(cmd).enqueue(incomingCommands)))
    } else {
      StateUpdateResult(DoNothing, RetryingFailedSends(sender, connector, nPendingSends - 1, pendingReplies, failedSends.enqueue(cmd), incomingCommands))
    }
  }




  case class ConnectionEstablished(sender: Sender, connector: Connector, nPendingSends: Int, pendingReplies: CommandQueue, failedSends: CommandQueue) extends ClientState {

    def failPendingAndReconnect(cause: Throwable) {
      pendingReplies.foreach(_._2.failure(cause))
      sender.close()
      connector.connectAsync()
    }

    override def send(cmd: CommandAndResponse) =
      StateUpdateResult(() => sender.send(cmd), new ConnectionEstablished(sender, connector, nPendingSends + 1, pendingReplies, failedSends) with EventLogger)

    override def commandSent(cmd: CommandAndResponse) =
      StateUpdateResult(DoNothing, new ConnectionEstablished(sender, connector, nPendingSends - 1, pendingReplies.enqueue(cmd), failedSends) with EventLogger)

    override def sendFailure(cause: Throwable, cmd: CommandAndResponse) = if(nPendingSends == 1) {
      StateUpdateResult(() => failPendingAndReconnect(cause), new WaitingForConnection(failedSends.enqueue(cmd)) with EventLogger)
    } else {
      StateUpdateResult(DoNothing, new ConnectionEstablished(sender, connector, nPendingSends - 1, pendingReplies, failedSends.enqueue(cmd)) with EventLogger)
    }

    override def responseReceived(response: String) = {
      val (hd, tl) = pendingReplies.dequeue
      StateUpdateResult(() => hd._2.success(response), new ConnectionEstablished(sender, connector, nPendingSends, tl, failedSends) with EventLogger)
    }

    override def receiveFailure(cause: Throwable): StateUpdateResult[ArtifactFunction] = if(nPendingSends == 0){
      StateUpdateResult(() => failPendingAndReconnect(cause), new WaitingForConnection(failedSends) with EventLogger)
    } else {
      StateUpdateResult(DoNothing, this)
    }
  }

  case class ConnectionBroken(cause: Throwable) extends ClientState {
    override def send(cmd: NettyClient.CommandAndResponse) = StateUpdateResult(() => cmd._2.failure(cause), this)

    override def connectionEstablished(sender: Sender, connector: Connector): StateUpdateResult[ArtifactFunction] =
      StateUpdateResult(DoNothing, new ConnectionEstablished(sender, connector, 0, Queue.empty, Queue.empty) with EventLogger)

    override def connectionBroken(cause: Throwable, connector: Connector): StateUpdateResult[ArtifactFunction] =
      StateUpdateResult(() => connector.scheduleConnect(5000), this)
  }


}




