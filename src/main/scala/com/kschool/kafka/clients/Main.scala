package com.kschool.kafka.clients

import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.io.Source

import com.kschool.kafka.clients.utils.Metrics
import com.kschool.kafka.common.models.Configuration
import io.circe.generic.auto._
import io.circe.parser._
import sun.misc.{Signal, SignalHandler}

object Main {
  val extraParallelism = 5
  def main(args: Array[String]): Unit = {
    val Array(configurationFile) = args
    val sourceConfig = Source.fromFile(configurationFile)

    try {
      val configuration = decode[Configuration](sourceConfig.mkString) match {
        case Left(error) =>
          throw new Exception(s"Error parsing configuration file: $error")
        case Right(configuration) => configuration
      }

      val totalParallelism =
          configuration.parallelism.consumer +
          configuration.parallelism.processor +
          configuration.parallelism.producer + extraParallelism

      implicit val ec = ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(totalParallelism))
      val kafkaService = new KafkaService(configuration)
      val f = kafkaService.start()

      Signal.handle(new Signal("INT"), new SignalHandler() {
        def handle(sig: Signal) {
          println(s"Shutdown KafkaService")
          kafkaService.stop()
          Metrics.show()
        }
      })

      Await.result(f, Duration.Inf)
    } finally sourceConfig.close()
  }
}
