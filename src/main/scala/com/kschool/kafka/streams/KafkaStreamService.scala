package com.kschool.kafka.streams

import java.time.Duration
import java.util.Properties

import com.kschool.kafka.clients.models._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Joined, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

class KafkaStreamService(configuration: Configuration) {

  val jsonSerializer = new JsonSerializer
  val jsonOptSerializer = new JsonOptSerializer
  val jsonDeserializer = new JsonDeserializer

  implicit val jsonSerde = Serdes.fromFn[Message](
    (topic: String, message: Message) => jsonSerializer.serialize(topic, message),
    (topic: String, bytes: Array[Byte]) => jsonDeserializer.deserialize(topic, bytes)
  )

  implicit val jsonOptSerde = Serdes.fromFn[Option[Message]](
    (topic: String, message: Option[Message]) => jsonOptSerializer.serialize(topic, message),
    (topic: String, bytes: Array[Byte]) => Option(jsonDeserializer.deserialize(topic, bytes))
  )

  implicit val messageConsumed = Consumed.`with`[String, Message](stringSerde, jsonSerde)
  implicit val messageProduced = Produced.`with`[String, Message](stringSerde, jsonSerde)
  implicit val messageOptProduced = Produced.`with`[String, Option[Message]](stringSerde, jsonOptSerde)
  implicit val messageJoined = Joined.`with`[String, Message, Message]

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

  val kStreams = new KafkaStreams(streams.build(), properties)

  def start(): Unit = kStreams.start()

  def stop(): Unit = kStreams.close(Duration.ofSeconds(60))

}
