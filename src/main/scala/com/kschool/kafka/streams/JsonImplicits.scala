package com.kschool.kafka.streams

import java.util.Properties

import com.kschool.kafka.common.models.{JsonDeserializer, JsonOptSerializer, JsonSerializer, Message}
import org.apache.kafka.streams.scala.kstream.{Consumed, Joined, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde

object JsonImplicits {
  val jsonSerializer = new JsonSerializer
  val jsonOptSerializer = new JsonOptSerializer
  val jsonDeserializer = new JsonDeserializer
  val avroOptSerializer = new JsonOptSerializer

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
}
