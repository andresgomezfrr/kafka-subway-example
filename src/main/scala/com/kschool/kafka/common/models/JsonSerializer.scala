package com.kschool.kafka.common.models

import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serializer

class JsonSerializer extends Serializer[Message] {
  override def serialize(topic: String, data: Message): Array[Byte] = data.asJson.noSpaces.getBytes
}
