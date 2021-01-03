package com.kschool.kafka.clients

import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.util.Random

import com.kschool.kafka.common.models._
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest._
import flatspec._
import matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

class KafkaServiceTest extends AnyFlatSpec with must.Matchers with EmbeddedKafka with BeforeAndAfterAll with Eventually {

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(20))
  implicit val keySerializer = new StringSerializer
  implicit val serializer = new JsonSerializer
  implicit val valueDeserializer = new JsonDeserializer
  implicit val config = EmbeddedKafkaConfig(Random.nextInt(300) + 5000, Random.nextInt(300) + 4000)
  val embeddedKafka = EmbeddedKafka.start()

  private def randomID: String = UUID.randomUUID().toString

  it must "generate metric and control message if user enter and exit and is not in the system" in {
    val userId = "UserA"
    val topicConfiguration = TopicConfiguration(randomID, randomID, randomID, randomID)
    val configuration = Configuration(kafkaBootstrapServers = s"localhost:${embeddedKafka.config.kafkaPort}", topics = topicConfiguration)
    val kafkaService = new KafkaService(configuration)

    createCustomTopic(topicConfiguration.in)
    createCustomTopic(topicConfiguration.control)
    createCustomTopic(topicConfiguration.metric)
    createCustomTopic(topicConfiguration.alert)

    kafkaService.start()

    publishToKafka[String, Message](topicConfiguration.in, userId, InMessage(1000, userId, "User A", 0))
    Thread.sleep(100)
    publishToKafka[String, Message](topicConfiguration.in, userId, InMessage(1100, userId, "User A", 1))

    val metricMessages = consumeNumberMessagesFrom[Option[Message]](topicConfiguration.metric, number = 1, autoCommit = true)

    metricMessages.size must be(1)
    metricMessages.head.isDefined must be(true)
    val metricMessage = metricMessages.head.get
    metricMessage.isInstanceOf[MetricMessage] must be(true)
    metricMessage.asInstanceOf[MetricMessage].user_id must be(userId)
    metricMessage.asInstanceOf[MetricMessage].timestamp must be(1100)
    metricMessage.asInstanceOf[MetricMessage].duration must be(100)

    val controlMessages = consumeNumberMessagesFrom[Option[Message]](topicConfiguration.control, number = 1, autoCommit = true)

    controlMessages.size must be(1)
    controlMessages.head.isDefined must be(true)
    val controlMessage = controlMessages.head.get
    controlMessage.isInstanceOf[ControlMessage] must be(true)
    controlMessage.asInstanceOf[ControlMessage].user_id must be(userId)
    controlMessage.asInstanceOf[ControlMessage].timestamp must be(1000)
    controlMessage.asInstanceOf[ControlMessage].action must be(0)

    kafkaService.stop()
  }

  override protected def afterAll(): Unit = {
    embeddedKafka.stop(clearLogs = true)
  }

}
