package com.kschool.kafka.exercise.consumer

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import com.kschool.kafka.exercise.models.{Configuration, JsonDeserializer, Message}
import com.kschool.kafka.exercise.utils.Metrics
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

class Consumer(id: Int,
               configuration: Configuration,
               inQueue: LinkedBlockingQueue[Message],
               isRunning: AtomicBoolean)(implicit ex: ExecutionContext) {
  println(s"Consumer[$id] - Create consumer")
  val topic = configuration.topics.in
  val props = new Properties()
  props.put(BOOTSTRAP_SERVERS_CONFIG, configuration.kafkaBootstrapServers)
  props.put(ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "100")
  props.put(GROUP_ID_CONFIG, configuration.applicationId)
  props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[JsonDeserializer].getName)

  val kafkaConsumer = new KafkaConsumer[String, Option[Message]](props)
  kafkaConsumer.subscribe(Seq(topic).asJava, new ConsumerRebalanceListener {
    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit =
      println(s"Consumer[$id] - Read from: ${partitions.asScala}")
  })

  def start(): Future[Unit] = Future {
    println(s"Consumer[$id] - Start consumer")

    try {
      while (isRunning.get()) {
        val records = kafkaConsumer.poll(Duration.ofMillis(500)).asScala.toSeq
        records.foreach { record =>
          record.value() match {
            case None => Metrics.updateMetric(s"consumer-errors-$topic")
            case Some(message) =>
              inQueue.put(message)
              Metrics.updateMetric(s"consumer-records")
              Metrics.updateMetric(s"consumer[$id]-records")
              Metrics.updateMetric(s"consumer-records-$topic")
              Metrics.updateMetric(s"consumer[$id]-records-$topic")
          }
        }
      }
    } finally {
      println(s"Consumer[$id] - Stop consumer")
      kafkaConsumer.close()
    }
  }


}
