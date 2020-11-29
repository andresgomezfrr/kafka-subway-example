package com.kschool.kafka.exercise.models

case class TopicConfiguration(in: String, control: String, alert: String, metric: String)

case class ParallelismConfiguration(consumer: Int, processor: Int, producer: Int)

case class Configuration(kafkaBootstrapServers: String,
                         topics: TopicConfiguration,
                         parallelism: ParallelismConfiguration)
