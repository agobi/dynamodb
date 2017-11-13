package com.github.agobi.dynamodb.alpakka

import java.util.concurrent.ThreadLocalRandom

import akka.stream.StreamTcpException
import com.amazonaws.SdkClientException
import com.amazonaws.retry.RetryUtils
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity

import scala.collection.immutable
import scala.concurrent.duration._

trait CapacityAware[T] {
  def consumedCapacity(t: T): immutable.Seq[ConsumedCapacity]
}

trait BackoffSettings {
  def unapply(arg: Throwable): Option[Int => FiniteDuration]
  def retryDelay(retryCount: Int): FiniteDuration
}

object Throttling {
  def unapply(arg: Throwable): Option[Throwable] = arg match {
    case (e: SdkClientException) if RetryUtils.isThrottlingException(e) =>
      Some(e)

    case _ =>
      None
  }
}

object Retryable {
  def unapply(arg: Throwable): Option[Throwable] = arg match {
    case (e: SdkClientException)
        if RetryUtils.isRetryableServiceException(e) ||
          RetryUtils.isClockSkewError(e) =>
      Some(e)

    // Should we match for connection reset?
    case (e: StreamTcpException) =>
      Some(e)

    case _ =>
      None
  }
}

case class AWSBackoff(
    minBackoff: FiniteDuration = 10.millis,
    throttleBackoff: FiniteDuration = 25.millis,
    maxBackoff: FiniteDuration = 30.second,
    randomFactor: Double = 0.5
) extends BackoffSettings {

  override def unapply(arg: Throwable): Option[Int => FiniteDuration] =
    arg match {
      case Throttling(_) => Some(calculate(_, throttleBackoff))
      case Retryable(_)  => Some(calculate(_, minBackoff))
      case _             => None
    }

  override def retryDelay(retryCount: Int): FiniteDuration =
    calculate(retryCount, throttleBackoff)

  // From akka.pattern.BackoffSupervisor
  @inline private def calculate(retryCount: Int, min: FiniteDuration): FiniteDuration = {
    val rnd = 1.0 + ThreadLocalRandom.current().nextDouble() * randomFactor
    if (retryCount >= 30) // Duration overflow protection (> 100 years)
      maxBackoff
    else
      maxBackoff.min(min * math.pow(2, retryCount)) * rnd match {
        case f: FiniteDuration ⇒ f
        case _ ⇒ maxBackoff
      }
  }
}

object AWSBackoff {
  lazy val Default = AWSBackoff()
}
