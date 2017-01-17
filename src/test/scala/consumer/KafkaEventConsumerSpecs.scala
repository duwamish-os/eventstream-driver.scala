package consumer

import java.util
import java.util.{Collections, Date, Properties}

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import producer.BaseEvent
import producer.kafka.KafkaEventPublisher

import scala.collection.JavaConverters._

/**
  * Created by prayagupd on 1/14/17.
  */

class TestEventKafkaEventConsumer[TestEvent] extends AbstractKafkaEventConsumer {

  addConfiguration("group.id", "consumer_group_test")
    .subscribeEvents(List("TestEvent"))

  override def consumeEvent(eventRecord: ConsumerRecord[String, String]): Unit = {
    println(s"Event = ${eventRecord.value()}")
  }
}

class KafkaEventConsumerSpecs extends FunSuite with BeforeAndAfterEach {

  implicit val streamingConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  case class TestEvent(eventOffset: Long, hashValue: Long, created: Date, testField: String) extends BaseEvent

  override protected def beforeEach(): Unit = {
    EmbeddedKafka.start()
  }

  override protected def afterEach(): Unit = {
    EmbeddedKafka.stop()
  }

  test("given an event in the event-store, consumes an event") {

    val event = TestEvent(0l, 0l, new Date(), "data")
    val config = new Properties() {
      {
        load(this.getClass.getResourceAsStream("/producer.properties"))
      }
    }
    val producer = new KafkaProducer[String, String](config)

    val persistedEvent = producer.send(new ProducerRecord(event.getClass.getSimpleName, event.toString))

    assert(persistedEvent.get().offset() == 0)
    assert(persistedEvent.get().checksum() != 0)

    val consumerConfig = new Properties() {
      {
        put("group.id", "consumers_testEventsGroup")
        put("client.id", "testEventConsumer")
        put("auto.offset.reset", "earliest")
      }
    }

    assert(consumerConfig.getProperty("group.id") == "consumers_testEventsGroup")

    val kafkaConsumer = new TestEventKafkaEventConsumer[TestEvent].addConfiguration(consumerConfig)

    //assert(kafkaConsumer.listTopics().asScala.map(_._1).toList == List("TestEvent"))

    kafkaConsumer.subscribeEvents(List("TestEvent"))

    //assert(kafkaConsumer.partitionsFor("TestEvent").asScala.map(_.partition()).toList == List(0))

    val events = kafkaConsumer.consumeAll()
  }
}
