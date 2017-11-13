package com.github.agobi.dynamodb

import java.time.Instant

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.alpakka.dynamodb.AwsOp
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits.{BatchGetItem, BatchWriteItem, Query, Scan}
import akka.stream.scaladsl.{Flow, GraphDSL, Source}
import akka.stream.{FlowShape, ThrottleMode}
import com.amazonaws.services.dynamodbv2.model._
import com.github.agobi.dynamodb.alpakka.Retry.Counters

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.concurrent.Future
import scala.concurrent.duration._

package object alpakka {
  type AttributeValues = java.util.Map[String, AttributeValue]
  type RetryStats = () => Future[Counters]

  val defaultBatchTimeout: FiniteDuration = 10.millis
  val defaultParallelism = 20

  private val AwsMaxWriteSize = 25
  private val AwsMaxReadSize = 100

  implicit val scanResultIsCapacityAware: CapacityAware[ScanResult] =
    new CapacityAware[ScanResult] {
      override def consumedCapacity(t: ScanResult): immutable.Seq[ConsumedCapacity] =
        Option(t.getConsumedCapacity).toList
    }

  implicit val batchGetItemResultIsCapacityAware: CapacityAware[BatchGetItemResult] =
    new CapacityAware[BatchGetItemResult] {
      override def consumedCapacity(t: BatchGetItemResult): immutable.Seq[ConsumedCapacity] =
        Option(t.getConsumedCapacity).map(_.asScala).toList.flatten
    }

  implicit val batchWriteItemResultIsCapacityAware: CapacityAware[BatchWriteItemResult] =
    new CapacityAware[BatchWriteItemResult] {
      override def consumedCapacity(t: BatchWriteItemResult): immutable.Seq[ConsumedCapacity] =
        Option(t.getConsumedCapacity).map(_.asScala).toList.flatten
    }


  def paginate[REQ <: AwsOp](
    client: DynamoClient,
    backoff: BackoffSettings,
    request: REQ,
    offset: REQ#B => Option[REQ]
  )(
    implicit system: ActorSystem
  ): Source[REQ#B, NotUsed] = {
    import system.dispatcher

    Source.unfoldAsync[Option[REQ], REQ#B](Some(request)) {
      case Some(req) =>
        Retry.retry[REQ#B](() => client.single(req), backoff).map { res =>
          Some(offset(res) -> res)
        }
      case None =>
        Future.successful(None)
    }
  }

  def batched[IN, OUT, REQ <: AwsOp](
    client: DynamoClient,
    parallelism: Int,
    backoff: BackoffSettings,
    batchSize: Int,
    timeout: FiniteDuration,
    request: immutable.Seq[(IN, Int)] => REQ,
    recombine: (REQ#B, immutable.Seq[(IN, Int)]) => (OUT, immutable.Seq[(IN, Int)])
  )(
    implicit system: ActorSystem
  ): Flow[IN, OUT, RetryStats] =
    Flow.fromGraph(GraphDSL.create(Retry[(IN, Int), Option[OUT]](4 * batchSize)) { implicit b => retry =>
      import GraphDSL.Implicits._
      import Retry.logSource
      import system.dispatcher

      val adapter = Logging(system, this)

      def calculateRetryTime(delay: Int => FiniteDuration, data: (IN, Int)): (Instant, (IN, Int)) = {
        val ms = delay(data._2).toMillis
        adapter.debug(s"Retry data in {}ms, retryCount: {}", ms, data._2)
        Instant.now().plusMillis(ms) -> (data._1 -> (data._2 + 1))
      }

      val firstTry = b.add(Flow.fromFunction { (in: IN) =>
        in -> 0
      })

      retry <~> Flow[(IN, Int)]
        .groupedWithin(batchSize, timeout)
        .map { (data: immutable.Seq[(IN, Int)]) =>
          request(data) -> data
        }
        .mapAsyncUnordered(parallelism) {
          case (req, pt) =>
            client
              .single(req)
              .map { result =>
                val (out, retries) = recombine(result, pt)
                Retry.Processed[(IN, Int), Option[OUT]](
                  Some(out),
                  retries.map(calculateRetryTime(backoff.retryDelay, _)),
                  pt.size)
              }
              .recover {
                case e @ backoff(delay) =>
                  adapter.debug(s"Query failed with message {}", e.getMessage)
                  Retry.Processed[(IN, Int), Option[OUT]](
                    None,
                    pt.map(calculateRetryTime(delay, _)),
                    pt.size)
              }
        }

      firstTry ~> retry.in1

      FlowShape(firstTry.in, retry.out2.mapConcat(_.toIndexedSeq).outlet)
    })

  def dynamoThrottle[T](units: Int)(implicit capacity: CapacityAware[T]): Flow[T, T, NotUsed] = {
    Flow[T].throttle(
      (units * 10).max(1),
      1.second,
      Math.round(units.max(5)), { r =>
        val consumed: Double = capacity
          .consumedCapacity(r)
          .map(_.getCapacityUnits.toDouble)
          .sum
          .max(0.1)
        Math.round(consumed * 10).toInt
      },
      ThrottleMode.Shaping
    )
  }

  def scan(client: DynamoClient)
          (request: ScanRequest, backoff: BackoffSettings = AWSBackoff.Default)
          (implicit system: ActorSystem): Source[ScanResult, NotUsed] =
    paginate[Scan](client, backoff, request.toOp, { result =>
      Option(result.getLastEvaluatedKey)
        .map(request.withExclusiveStartKey(_).toOp)
    })

  def query(client: DynamoClient)
           (request: QueryRequest, backoff: BackoffSettings = AWSBackoff.Default)
           (implicit system: ActorSystem): Source[QueryResult, NotUsed] =
    paginate[Query](client, backoff, request.toOp, { result =>
      Option(result.getLastEvaluatedKey)
        .map(request.withExclusiveStartKey(_).toOp)
    })

  def write(client: DynamoClient)
           (parallelism: Int = defaultParallelism, backoff: BackoffSettings = AWSBackoff.Default, timeout: FiniteDuration = defaultBatchTimeout)
           (convert: BatchWriteItemRequest => BatchWriteItemRequest = identity)
           (implicit system: ActorSystem): Flow[(String, WriteRequest), BatchWriteItemResult, RetryStats] =
    batched[(String, WriteRequest), BatchWriteItemResult, BatchWriteItem](
      client,
      parallelism,
      backoff,
      AwsMaxWriteSize,
      timeout, { data =>
        convert(
          new BatchWriteItemRequest(
            data.groupBy(_._1._1).mapValues(_.map(_._1._2).asJava).asJava)).toOp
      }, {
        case (data, pt) =>
          val unprocessed: immutable.Seq[((String, WriteRequest), Int)] =
            data.getUnprocessedItems.asScala.toIndexedSeq.flatMap {
              case (tableName, reqList) =>
                val req: mutable.Seq[(String, WriteRequest)] =
                  reqList.asScala.map(tableName -> (_: WriteRequest))
                req.map(r => pt.find(_._1 == r).get)
            }

          data -> unprocessed
      }
    )

  def readBase(client: DynamoClient)
              (parallelism: Int = defaultParallelism, backoff: BackoffSettings = AWSBackoff.Default, timeout: FiniteDuration = defaultBatchTimeout)
              (request: BatchGetItemRequest => BatchGetItemRequest = identity)
              (implicit system: ActorSystem): Flow[(String, AttributeValues), BatchGetItemResult, RetryStats] = {
    batched[(String, AttributeValues), BatchGetItemResult, BatchGetItem](
      client,
      parallelism,
      backoff,
      AwsMaxReadSize,
      timeout, { data =>
        request(
          new BatchGetItemRequest(
            data
              .groupBy(_._1._1)
              .mapValues(attributeValues =>
                new KeysAndAttributes().withKeys(
                  attributeValues.map(_._1._2).asJavaCollection))
              .asJava)).toOp
      }, { (result, pt) =>
        val unprocessed: immutable.Seq[((String, AttributeValues), Int)] =
          result.getUnprocessedKeys.asScala.toIndexedSeq.flatMap {
            case (tableName, reqList) =>
              val req = reqList.getKeys.asScala.map(tableName -> _)
              req.map(r => pt.find(_._1 == r).get)
          }

        result -> unprocessed
      }
    )
  }

}
