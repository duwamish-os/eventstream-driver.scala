package producer.kafka

import java.util
import java.util.{Date, Properties}

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import producer.BaseEvent

import scala.collection.JavaConverters._

/**
  * Created by prayagupd
  * on 1/14/17.
  */

class KafkaEventPublisherIntegrationSpecs extends FunSuite with BeforeAndAfterEach {

  case class ItemOrderedEvent(eventOffset: Long, hashValue: Long, eventType: String, created: Date) extends BaseEvent {
    override def fromPayload(payload: String): BaseEvent = {null}
  }

  implicit val streamingConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  val kafkaPublisher = new KafkaEventPublisher

  override protected def beforeEach(): Unit = EmbeddedKafka.start()

  override protected def afterEach(): Unit = EmbeddedKafka.stop()

  test("produces a record to kafka store") {

    val event = ItemOrderedEvent(0l, 0l, classOf[ItemOrderedEvent].getSimpleName, new Date())

    val persistedEvent = kafkaPublisher.publish(event)
    assert(persistedEvent.eventOffset == 0)
    assert(persistedEvent.hashValue != 0)

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

    val topics = kafkaConsumer.listTopics().asScala

    assert(topics.map(_._1) == List(classOf[ItemOrderedEvent].getSimpleName))

    kafkaConsumer.subscribe(util.Arrays.asList(classOf[ItemOrderedEvent].getSimpleName))

    val topic = AdminUtils.topicExists(new ZkUtils(new ZkClient("localhost:2181", 10000, 15000),
      new ZkConnection("localhost:2181"), false), classOf[ItemOrderedEvent].getSimpleName)

    assert(topic)

    var events: ConsumerRecords[String, String] = null

    events = kafkaConsumer.poll(1000)
    println(events.partitions().size())

    assert(events.count() == 1)
  }

  test("produces multiple records to kafka store") {

    val event = ItemOrderedEvent(0l, 0l, classOf[ItemOrderedEvent].getSimpleName, new Date())

    val persistedEvent = kafkaPublisher.publish(event)
    assert(persistedEvent.eventOffset == 0)
    assert((persistedEvent.hashValue + "").length > 0)

    val persistedEvent2 = kafkaPublisher.publish(ItemOrderedEvent(0l, 0l, classOf[ItemOrderedEvent].getSimpleName, new Date()))
    assert(persistedEvent2.eventOffset == 1)
    assert((persistedEvent2.hashValue + "").length > 0)
  }
}
