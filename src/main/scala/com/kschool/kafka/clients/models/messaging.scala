package com.kschool.kafka.clients.models

sealed trait Message {
  val timestamp: Long
  val user_id: String
  val full_name: String
}

case class UserMessage(timestamp: Long, user_id: String, full_name: String) extends Message

object UserMessage {

}

case class InMessage(timestamp: Long, user_id: String, full_name: String, action: Int) extends Message

case class ControlMessage(timestamp: Long, user_id: String, full_name: String, action: Int) extends Message

case class AlertMessage(timestamp: Long, user_id: String, full_name: String, action: Int) extends Message

case class MetricMessage(timestamp: Long, user_id: String, full_name: String, duration: Long) extends Message
