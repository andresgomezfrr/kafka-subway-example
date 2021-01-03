package com.kschool.kafka.common.models

import java.util.UUID

case class TopicConfiguration(in: String, control: String, alert: String, metric: String)

case class ParallelismConfiguration(consumer: Int, processor: Int, producer: Int)

case class Configuration(applicationId: String = UUID.randomUUID().toString,
                         kafkaBootstrapServers: String,
                         topics: TopicConfiguration,
                         parallelism: ParallelismConfiguration = ParallelismConfiguration(1, 1, 1))
