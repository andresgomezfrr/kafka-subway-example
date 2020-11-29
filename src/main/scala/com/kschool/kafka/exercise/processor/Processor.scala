package com.kschool.kafka.exercise.processor

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

import com.kschool.kafka.exercise.models.{AlertMessage, Configuration, ControlMessage, InMessage, Message, MetricMessage}
import com.kschool.kafka.exercise.utils.{MapCache, Metrics}

class Processor(id: Int,
                configuration: Configuration,
                inQueue: LinkedBlockingQueue[Message],
                outQueue: LinkedBlockingQueue[(String, Message)],
                isRunning: AtomicBoolean)(implicit ex: ExecutionContext) {

  def start(): Future[Unit] = Future {
    println(s"Processor[$id] - Start processor")
    while (isRunning.get()) {
      val message = inQueue.poll(10, TimeUnit.SECONDS)

      Option(message) match {
        case Some(InMessage(timestamp, userId, fullName, action)) =>
          MapCache.getUser(userId) match {
            case Some(lastSeen) =>
              action match {
                case 0 =>
                  outQueue.put(configuration.topics.alert -> AlertMessage(timestamp, userId, fullName, 0))
                  outQueue.put(configuration.topics.control -> ControlMessage(timestamp, userId, fullName, 1))
                case 1 =>
                  outQueue.put(configuration.topics.metric -> MetricMessage(timestamp, userId, fullName, timestamp - lastSeen))
                  outQueue.put(configuration.topics.control -> ControlMessage(timestamp, userId, fullName, 0))
                  MapCache.removeUser(userId)
              }
            case None =>
              action match {
                case 0 =>
                  outQueue.put(configuration.topics.control -> ControlMessage(timestamp, userId, fullName, 0))
                  MapCache.addUser(userId, timestamp)
                case 1 =>
                  outQueue.put(configuration.topics.control -> ControlMessage(timestamp, userId, fullName, 1))
                  outQueue.put(configuration.topics.alert -> AlertMessage(timestamp, userId, fullName, 1))
              }
          }
        case None =>
        case m =>
          println(s"Error processing message in processor[$id]: Unexpected message[$m]")
      }

      Metrics.updateMetric(s"processor-records")
      Metrics.updateMetric(s"processor[$id]-records")
    }
    println(s"Processor[$id] - End processor")
  }


}
