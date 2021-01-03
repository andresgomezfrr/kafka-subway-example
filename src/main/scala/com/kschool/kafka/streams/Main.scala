package com.kschool.kafka.streams

import scala.io.Source

import com.kschool.kafka.clients.models.Configuration
import com.kschool.kafka.clients.utils.Metrics
import io.circe.generic.auto._
import io.circe.parser.decode
import sun.misc.{Signal, SignalHandler}

object Main {
  def main(args: Array[String]): Unit = {
    val Array(configurationFile) = args
    val sourceConfig = Source.fromFile(configurationFile)

    try {
      val configuration = decode[Configuration](sourceConfig.mkString) match {
        case Left(error) =>
          throw new Exception(s"Error parsing configuration file: $error")
        case Right(configuration) => configuration
      }

      val kafkaStreamService = new KafkaStreamService(configuration)
      kafkaStreamService.start()

      Signal.handle(new Signal("INT"), new SignalHandler() {
        def handle(sig: Signal) {
          println(s"Shutdown KafkaService")
          Metrics.show()
          kafkaStreamService.stop()
        }
      })

    } finally sourceConfig.close()

  }
}
