package com.github.agobi.dynamodb.alpakka

import java.time.Instant

import akka.actor.ActorSystem
import akka.event.{LogSource, Logging}
import akka.pattern.after
import akka.stream.scaladsl.BidiFlow
import akka.stream.stage._
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import Retry.{Counters, Processed}

import scala.collection.{immutable, mutable}
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

/**
  * BidiShape for proper retry logic.
  *
  *   +---------+
  *   |         |    +---------+
  * ~>|in subOut| ~> |         |
  *   |         |    | process |
  * <~|out subIn| <~ |         |
  *   |         |    +---------+
  *   +---------+
  *
  */
class Retry[IN, OUT](bufferSize: Int) extends GraphStageWithMaterializedValue[BidiShape[IN, IN, Processed[IN, OUT], OUT], () => Future[Retry.Counters]] {
  val in: Inlet[IN] = Inlet[IN]("Retry.in")
  val out: Outlet[OUT] = Outlet[OUT]("Retry.out")
  val subIn: Inlet[Processed[IN, OUT]] = Inlet[Processed[IN, OUT]]("Retry.subIn")
  val subOut: Outlet[IN] = Outlet[IN]("Retry.subOut")

  override def shape = BidiShape(in, subOut, subIn, out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, () => Future[Counters]) = {
    val logic = new Logic
    (logic, logic.counters)
  }

  private class Logic extends TimerGraphStageLogicWithLogging(shape) {
    private var counter: Int = 0
    private var terminating: Boolean = false
    private val inprogress = mutable.PriorityQueue[(Instant, IN)]()(Ordering.by(_._1))

    private val countersAsync = getAsyncCallback[Promise[Retry.Counters]] {

      _.success(Counters(
        inflight = counter,
        retry = inprogress.size,
        inprogress.headOption.map(d => (d._1.toEpochMilli - Instant.now().toEpochMilli).millis)
      ))
    }

    def counters(): Future[Counters] = {
      val p = Promise[Counters]
      countersAsync.invoke(p)
      p.future
    }

    override protected def onTimer(timerKey: Any): Unit = {
      if (isAvailable(subOut)) pushSubOut()
    }

    /**
      * SubOut is pulled, so we will try to push from the queue.
      * If there is no data available, schedule a push.
      *
      */
    def pushSubOut(): Boolean = {
      inprogress.headOption match {
        case Some((time, data)) =>
          if (time.minusMillis(5).isBefore(Instant.now())) {
            inprogress.dequeue()

            push(subOut, data)
            counter += 1
            tryCompleteSubOut()
            true
          } else {
            // check if there is a scheduled timer?
            scheduleOnce(Retry.retryTimerKey, (time.toEpochMilli - Instant.now().toEpochMilli).millis)

            false
          }

        case _ =>
          false
      }
    }

    def tryCompleteSubOut(): Unit = {
      if (terminating && counter == 0 && inprogress.isEmpty) {
        complete(subOut)
      }
    }

    setHandlers(in, subOut, new InHandler with OutHandler {
      override def onPush(): Unit = {
        inprogress += ((Instant.now(), grab(in)))
        if (isAvailable(subOut))
          pushSubOut()
      }

      override def onPull(): Unit = {
        if (!pushSubOut() && inprogress.size < bufferSize && !hasBeenPulled(in)) {
          tryPull(in)
        }
      }

      override def onUpstreamFinish(): Unit = {
        terminating = true
        tryCompleteSubOut()
      }
    })

    setHandlers(subIn, out, new InHandler with OutHandler {
      override def onPush(): Unit = {
        val data = grab(subIn)
        counter -= data.size
        push(out, data.processed)

        inprogress ++= data.retry
        if (isAvailable(subOut))
          pushSubOut()
      }

      override def onPull(): Unit = {
        pull(subIn)
      }
    })
  }
}

object Retry {
  private val retryTimerKey = "RetryTimerKey"

  private[alpakka] implicit val logSource: LogSource[AnyRef] = new LogSource[AnyRef] {
    def genString(o: AnyRef): String = o.getClass.getName
    override def getClazz(o: AnyRef): Class[_] = o.getClass
  }


  case class Counters(inflight: Int, retry: Int, next: Option[FiniteDuration])
  case class Processed[IN, OUT](processed: OUT, retry: immutable.Seq[(Instant, IN)], size: Int)


  def apply[IN, OUT](bufferSize: Int): BidiFlow[IN, IN, Processed[IN, OUT], OUT, () => Future[Counters]] =
    BidiFlow.fromGraph(new Retry[IN, OUT](bufferSize))


  def retry[T](f: () => Future[T], backoff: BackoffSettings)(implicit system: ActorSystem): Future[T] = {
    import system.dispatcher

    val adapter = Logging(system, this)

    @volatile var retryCount = 0
    def r(): Future[T] = f() recoverWith {
      case e@backoff(delay) =>
        val d = delay(retryCount)
        adapter.debug(s"Got exception, retrying ({}) in {}ms: {}", retryCount, d.toMillis, e.getMessage)
        retryCount += 1
        after(d, system.scheduler)(r())
    }
    r()
  }

}
