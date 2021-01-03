package com.kschool.kafka.clients.models

import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serializer

class JsonOptSerializer extends Serializer[Option[Message]] {
  override def serialize(topic: String, data: Option[Message]): Array[Byte] = data match {
    case Some(message) => message.asJson.noSpaces.getBytes
    case None => null
  }
}
