package com.kschool.kafka.exercise

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

import com.kschool.kafka.exercise.consumer.Consumer
import com.kschool.kafka.exercise.models.{Configuration, Message}
import com.kschool.kafka.exercise.processor.Processor
import com.kschool.kafka.exercise.producer.Producer

class KafkaService(configuration: Configuration)(implicit ex: ExecutionContext) {
  val isRunning: AtomicBoolean = new AtomicBoolean(true)
  val inQueue: LinkedBlockingQueue[Message] = new LinkedBlockingQueue[Message](1000)
  val outQueue: LinkedBlockingQueue[(String, Message)] = new LinkedBlockingQueue[(String, Message)](1000)

  def start(): Future[_] = {
    val consumers = (0 until configuration.parallelism.consumer).map { id =>
      new Consumer(id, configuration.kafkaBootstrapServers, configuration.topics.in, inQueue, isRunning).start()
    }

    val processors = (0 until configuration.parallelism.processor).map { id =>
      new Processor(id, configuration, inQueue, outQueue, isRunning).start()
    }

    val producers = (0 until configuration.parallelism.producer).map { id =>
      new Producer(id, configuration.kafkaBootstrapServers, outQueue, isRunning).start()
    }


    val futures = consumers ++ processors ++ producers
    Future.sequence(futures)
  }

  def stop(): Unit = {
    isRunning.set(false)
  }

}


