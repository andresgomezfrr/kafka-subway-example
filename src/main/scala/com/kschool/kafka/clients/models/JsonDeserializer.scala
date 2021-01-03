package com.kschool.kafka.clients.models

import io.circe.generic.auto._
import io.circe.parser._
import org.apache.kafka.common.serialization.Deserializer

class JsonDeserializer extends Deserializer[Option[Message]] {
  override def deserialize(topic: String, data: Array[Byte]): Option[Message] = {
    Option(data) match {
      case None => None
      case Some(array) =>
        decode[Message](array.map(_.toChar).mkString) match {
          case Left(error) =>
            println(s"Error parsing message: $error. Raw data: ${data.toSeq}")
            None
          case Right(value) => Option(value)
        }
    }
  }
}
