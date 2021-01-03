package com.kschool.kafka.clients.producer

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}

import com.kschool.kafka.common.models.{JsonSerializer, Message}
import com.kschool.kafka.clients.utils.Metrics
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class Producer(id: Int,
               kafkaServer: String,
               outQueue: LinkedBlockingQueue[(String, Message)],
               isRunning: AtomicBoolean)(implicit ex: ExecutionContext) {

  val props = new Properties()
  props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
  props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[JsonSerializer].getName)

  val kafkaProducer = new KafkaProducer[String, Message](props)

  def start(): Future[Unit] = Future {
    println(s"Producer[$id] - Start producer")
    try {
      while (isRunning.get()) {
        Option(outQueue.poll(10, TimeUnit.SECONDS)) match {
          case Some((topic, message)) =>
            kafkaProducer.send(new ProducerRecord[String, Message](topic, message.user_id, message))
            Metrics.updateMetric(s"producer-records")
            Metrics.updateMetric(s"producer[$id]-records")
            Metrics.updateMetric(s"producer-records-$topic")
            Metrics.updateMetric(s"producer[$id]-records-$topic")
          case None =>
        }
      }
    } finally {
      kafkaProducer.flush()
      kafkaProducer.close()
    }
    println(s"Producer[$id] - Stop producer")
  }
}
