package com.kschool.kafka.streams

import java.util.Properties
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}

import com.kschool.kafka.common.models._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Joined, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

class KafkaStreamService(configuration: Configuration) {

  import JsonImplicits._

  val streams = new StreamsBuilder()

  val userTable = streams
    .table[String, Message]("user-table")

  val Array(table, control, alert, metric) = streams
    .stream[String, Message](configuration.topics.in)
    .selectKey((_, inMessage) => inMessage.user_id)
    .leftJoin(userTable) { (message, userState) =>
      val inMessage = message.asInstanceOf[InMessage]

      Option(userState) match {
        case Some(state) =>
          if (inMessage.action == 0)
            Seq(
              ("control", Option(ControlMessage(message.timestamp, message.user_id, message.full_name, 1))),
              ("alert", Option(AlertMessage(message.timestamp, message.user_id, message.full_name, 1))),
            )
          else
            Seq(
              ("control", Option(ControlMessage(message.timestamp, message.user_id, message.full_name, 0))),
              ("metric", Option(MetricMessage(message.timestamp, message.user_id, message.full_name, inMessage.timestamp - state.timestamp))),
              ("table", None)
            )
        case None =>
          Seq(
            ("table", Option(message)),
            ("control", Option(ControlMessage(message.timestamp, message.user_id, message.full_name, 0)))
          )
      }
    }
    .flatMapValues((_, messages) => messages)
    .branch(
      (_, data) => data._1 == "table",
      (_, data) => data._1 == "control",
      (_, data) => data._1 == "alert",
      (_, data) => data._1 == "metric"
    )


  table
    .mapValues(_._2)
    .to("user-table")

  control
    .mapValues(_._2)
    .to(configuration.topics.control)

  alert
    .mapValues(_._2)
    .to(configuration.topics.alert)

  metric
    .mapValues(_._2)
    .to(configuration.topics.metric)

  val properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, configuration.applicationId)
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.kafkaBootstrapServers)
  properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, configuration.parallelism.processor.toString)
  properties.put("consumer." + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val kStreams = new KafkaStreams(streams.build(), properties)

  kStreams.setUncaughtExceptionHandler { (_: Thread, e: Throwable) =>
    e.printStackTrace()
    stop()
  }

  val p = Promise[Unit]
  kStreams.setStateListener {
    (newState: KafkaStreams.State, _: KafkaStreams.State) => {
      newState match {
        case KafkaStreams.State.RUNNING =>
          println("State running!!")
          p.success(())
        case KafkaStreams.State.ERROR =>
          p.failure(throw new Exception("Error state"))
        case _ =>
          println(s"Current state is: $newState")
      }
    }
  }

  def start(): Future[Unit] = {
    kStreams.start()
    p.future
  }

  val timeout = 1 minute

  def stop(): Unit = kStreams.close(java.time.Duration.ofSeconds(timeout.toSeconds))

}
