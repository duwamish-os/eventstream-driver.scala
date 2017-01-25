package consumer.kafka

import java.util.{Date, Properties}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import consumer.EventHandler
import event.BaseEvent
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by prayagupd
  * on 1/14/17.
  */

class KafkaSingleEventConsumerIntegrationSpecs extends FunSuite with BeforeAndAfterEach {

  implicit val streamingConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  override protected def beforeEach(): Unit = {
    EmbeddedKafka.start()
  }

  override protected def afterEach(): Unit = {
    EmbeddedKafka.stop()
  }

  test("given an event in the event-store, consumes an event") {

    val event = TestHappenedConsumerEvent(0l, 0l, classOf[TestHappenedConsumerEvent].getSimpleName, new Date(), "data1")

    val producer = new KafkaProducer[String, String](new Properties() {{
      load(this.getClass.getResourceAsStream("/producer.properties"))
    }})

    val persistedEvent = producer.send(new ProducerRecord("someEventStream", event.toString))

    assert(persistedEvent.get().offset() == 0)
    assert(persistedEvent.get().checksum() != 0)

    val kafkaConsumer = new AbstractKafkaSingleEventConsumer[TestHappenedConsumerEvent](List("someEventStream")) {
      addConfiguration(new Properties() {{
        put("group.id", "consumers_testEventsGroup")
        put("client.id", "testKafkaEventConsumer")
        put("auto.offset.reset", "earliest")
      }})
        .subscribeEventsInStream(classOf[TestHappenedConsumerEvent])
        .setEventHandler(new TestEventHandler)

      assert(getConfiguration.getProperty("group.id") == "consumers_testEventsGroup")
      assert(getConfiguration.getProperty("client.id") == "testKafkaEventConsumer")
    }

    assert(kafkaConsumer.getExistingEventStreams() == List("someEventStream"))

    val events = kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 1)
  }

  test("given 2 events in the event-store, consumes events and updates the offset") {

    val kafkaConsumer = new AbstractKafkaSingleEventConsumer[TestHappenedConsumerEvent](List("someEventStream")) {
      addConfiguration(new Properties() {{
        put("group.id", "consumers_testEventsGroup")
        put("client.id", "testKafkaEventConsumer")
        put("auto.offset.reset", "latest")
      }})
        .subscribeEventsInStream(classOf[TestHappenedConsumerEvent])
        .setEventHandler(new TestEventHandler)

      assert(getConfiguration.getProperty("group.id") == "consumers_testEventsGroup")
      assert(getConfiguration.getProperty("client.id") == "testKafkaEventConsumer")
    }
    kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 0)
    assert(kafkaConsumer.getExistingEventStreams() == List("someEventStream", "__consumer_offsets"))

    val producer = new KafkaProducer[String, String](new Properties() {{
      load(this.getClass.getResourceAsStream("/producer.properties"))
    }})

    val persistedEvent = producer.send(new ProducerRecord("someEventStream",
      TestHappenedConsumerEvent(0l, 0l, classOf[TestHappenedConsumerEvent].getSimpleName, new Date(), "{data2}").toString))
    assert(persistedEvent.get().offset() == 0)
    assert(persistedEvent.get().checksum() != 0)

    assert(kafkaConsumer.getConsumerPosition == 0)
    val events1 = kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 1)

    val persistedEvent2 = producer.send(new ProducerRecord("someEventStream",
      TestHappenedConsumerEvent(0l, 0l, classOf[TestHappenedConsumerEvent].getSimpleName, new Date(), "{data3}").toString))
    assert(persistedEvent2.get().offset() == 1)
    assert(persistedEvent2.get().checksum() != 0)

    assert(kafkaConsumer.getExistingEventStreams() == List("someEventStream", "__consumer_offsets"))

    val events2 = kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 2)
  }
}


@JsonIgnoreProperties(Array("eventOffset", "eventHashValue"))
case class TestHappenedConsumerEvent(eventOffset: Long, eventHashValue: Long, eventType: String,
                                     createdDate: Date, field1: String) extends BaseEvent {

  def this() {
    this(0, 0, "", new Date(), "")
  }

  override def toString: String = toJSON(this.copy())

  override def copyy(eventOffset: Long, eventHashValue: Long): BaseEvent = {
    this.copy(eventOffset = eventOffset, eventHashValue = eventHashValue)
  }
}

class TestEventHandler extends EventHandler[TestHappenedConsumerEvent] {

  override def onEvent(event: TestHappenedConsumerEvent): Unit = {

    Thread.sleep(3000)
    println(s"event = ${event} => ${new Date()}")
  }
}
