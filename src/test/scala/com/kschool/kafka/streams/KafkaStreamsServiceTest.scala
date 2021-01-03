package com.kschool.kafka.streams

import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.{Random, Try}

import com.kschool.kafka.common.models._
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec._
import org.scalatest.matchers._

class KafkaStreamsServiceTest extends AnyFlatSpec with must.Matchers with EmbeddedKafka with BeforeAndAfterAll with Eventually {

  implicit val keySerializer = new StringSerializer
  implicit val serializer = new JsonSerializer
  implicit val valueDeserializer = new JsonDeserializer
  implicit val config = EmbeddedKafkaConfig(Random.nextInt(300) + 5000, Random.nextInt(300) + 4000)
  val embeddedKafka = EmbeddedKafka.start()

  private def randomID: String = UUID.randomUUID().toString

  it must "generate metric and control message if user enter and and exit is not in the system" in {
    val userId = "UserA"
    val topicConfiguration = TopicConfiguration(randomID, randomID, randomID, randomID)
    val configuration = Configuration(kafkaBootstrapServers = s"localhost:${embeddedKafka.config.kafkaPort}", topics = topicConfiguration)
    val kafkaService = new KafkaStreamService(configuration)

    createCustomTopic(topicConfiguration.in)
    createCustomTopic(topicConfiguration.control)
    createCustomTopic(topicConfiguration.metric)
    createCustomTopic(topicConfiguration.alert)
    createCustomTopic("user-table")

    val f = kafkaService.start()
    try {
      Await.result(f, Duration.Inf)

      publishToKafka[String, Message](topicConfiguration.in, userId, InMessage(1000, userId, "User A", 0))
      Thread.sleep(1500)
      publishToKafka[String, Message](topicConfiguration.in, userId, InMessage(2500, userId, "User A", 1))

      val metricMessages = consumeNumberMessagesFromTopics[Option[Message]](Set(topicConfiguration.metric), number = 1, autoCommit = true, 30 seconds)
        .getOrElse(topicConfiguration.metric, Seq.empty)

      metricMessages.size must be(1)
      metricMessages.head.isDefined must be(true)
      val metricMessage = metricMessages.head.get
      metricMessage.isInstanceOf[MetricMessage] must be(true)
      metricMessage.asInstanceOf[MetricMessage].user_id must be(userId)
      metricMessage.asInstanceOf[MetricMessage].timestamp must be(2500)
      metricMessage.asInstanceOf[MetricMessage].duration must be(1500)

      val controlMessages = consumeNumberMessagesFromTopics[Option[Message]](Set(topicConfiguration.control), number = 1, autoCommit = true, 30 seconds)
        .getOrElse(topicConfiguration.control, Seq.empty)

      controlMessages.size must be(1)
      controlMessages.head.isDefined must be(true)
      val controlMessage = controlMessages.head.get
      controlMessage.isInstanceOf[ControlMessage] must be(true)
      controlMessage.asInstanceOf[ControlMessage].user_id must be(userId)
      controlMessage.asInstanceOf[ControlMessage].timestamp must be(1000)
      controlMessage.asInstanceOf[ControlMessage].action must be(0)
    } finally kafkaService.stop()
  }

  override protected def afterAll(): Unit = {
    embeddedKafka.stop(clearLogs = true)
  }

}
