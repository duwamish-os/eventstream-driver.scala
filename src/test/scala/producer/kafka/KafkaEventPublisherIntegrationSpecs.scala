package producer.kafka

import java.io.ByteArrayOutputStream
import java.util
import java.util.{Date, Properties}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import event.{BaseEvent, EventOffsetAndHashValue}
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.JavaConverters._

/**
  * Created by prayagupd
  * on 1/14/17.
  */

class KafkaEventPublisherIntegrationSpecs extends FunSuite with BeforeAndAfterEach {

  implicit val streamingConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  override protected def beforeEach(): Unit = EmbeddedKafka.start()

  override protected def afterEach(): Unit = EmbeddedKafka.stop()

  test("produces a record to kafka store") {

    val event = ItemOrderedEvent(0l, 0l, classOf[ItemOrderedEvent].getSimpleName, new Date(2017, 10, 28))

    val kafkaPublisher = new KafkaEventPublisher
    val persistedEvent = kafkaPublisher.publish(event)
    assert(persistedEvent.eventOffset == 0)
    assert(persistedEvent.eventHashValue > 0)

    val consumerConfig = new Properties() {
      {
        put("bootstrap.servers", "localhost:9092") //streaming.config
        put("group.id", "consumer_group_test")
        put("auto.offset.reset", "earliest")
        put("key.deserializer", classOf[StringDeserializer].getName)
        put("value.deserializer", classOf[StringDeserializer].getName)
      }
    }

    val kafkaConsumer = new KafkaConsumer[String, String](consumerConfig)

    assert(kafkaConsumer.listTopics().asScala.map(_._1) == List(classOf[ItemOrderedEvent].getSimpleName))

    kafkaConsumer.subscribe(util.Arrays.asList(classOf[ItemOrderedEvent].getSimpleName))

    assert(AdminUtils.topicExists(new ZkUtils(new ZkClient("localhost:2181", 10000, 15000),
      new ZkConnection("localhost:2181"), false), classOf[ItemOrderedEvent].getSimpleName))

    val events: ConsumerRecords[String, String] = kafkaConsumer.poll(1000)

    assert(events.asScala.map(_.value()).toList.head == "{\"eventType\":\"ItemOrderedEvent\",\"createdDate\":61470000000000}")
    assert(events.partitions().size() == 1)
    assert(events.count() == 1)
  }

  test("produces multiple records to kafka store") {

    val event = ItemOrderedEvent(0l, 0l, classOf[ItemOrderedEvent].getSimpleName, new Date(2017, 10, 28))

    val kafkaPublisher = new KafkaEventPublisher
    val persistedEvent = kafkaPublisher.publish(event)
    assert(persistedEvent.eventOffset == 0)
    assert((persistedEvent.eventHashValue + "").length > 0)

    val persistedEvent2 = kafkaPublisher.publish(ItemOrderedEvent(0l, 0l, classOf[ItemOrderedEvent].getSimpleName, new Date()))
    assert(persistedEvent2.eventOffset == 1)
    assert((persistedEvent2.eventHashValue + "").length > 0)
  }
}

@JsonIgnoreProperties(Array("eventOffset", "eventHashValue"))
case class ItemOrderedEvent(eventOffset: Long, eventHashValue: Long, eventType: String, createdDate: Date) extends BaseEvent {
  override def fromPayload(offset: EventOffsetAndHashValue, payload: String): BaseEvent = {null}
}