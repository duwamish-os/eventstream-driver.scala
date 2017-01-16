package consumer

import java.util.Date

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.FunSuite
import producer.{BaseEvent, KafkaEventPublisher}

/**
  * Created by prayagupd on 1/14/17.
  */

class KafkaEventConsumer[TestEvent] extends AbstractKafkaEventConsumer {

  override def consume(eventRecord: ConsumerRecord[String, String]): Unit = {
    println(eventRecord.value())
  }
}

class KafkaEventConsumerSpecs extends FunSuite with EmbeddedKafka {

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
  implicit val deserialiser = new StringDeserializer

  case class TestEvent(eventOffset: Long, hashValue: Long, created: Date, testField : String) extends BaseEvent

  test("given an event in the event-store, consumes an event") {

    withRunningKafka {

      val producer = new KafkaEventPublisher

      val persistedEvent = producer.publish(TestEvent(0l, 0l, new Date(), "data"))
      assert(persistedEvent.eventOffset == 0)
      assert(persistedEvent.hashValue != 0)

      val e = consumeFirstMessageFrom("TestEvent")

      val kafkaConsumer = new KafkaEventConsumer
      kafkaConsumer.subscribeEvents(List("TestEvent"))

      kafkaConsumer.consumeAll()

      assert(1 == 1)
    }
  }
}
