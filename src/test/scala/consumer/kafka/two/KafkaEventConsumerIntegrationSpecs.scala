package consumer.kafka.two

import java.util.{Date, Properties}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import consumer.TwoEventsHandler
import event.BaseEvent
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by prayagupd
  * on 1/25/17.
  */

class TestTwoEventHandler extends TwoEventsHandler[Event1, Event2] {

  override def onEventA(event: Event1): Unit = {

    Thread.sleep(3000)
    println(s"event1 = ${event} => ${new Date()}")
  }

  override def onEventB(event: Event2): Unit = {

    Thread.sleep(3000)
    println(s"event2 = ${event} => ${new Date()}")
  }
}

class KafkaEventConsumerIntegrationSpecs extends FunSuite with BeforeAndAfterEach {

  implicit val streamingConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  override protected def beforeEach(): Unit = EmbeddedKafka.start()

  override protected def afterEach(): Unit = EmbeddedKafka.stop()

  test("given event1, one of two event types in event-stream, consumes it") {

    val event1 = Event1(0l, 0l, classOf[Event1].getName, new Date(), "data1")

    val producer = new KafkaProducer[String, String](new Properties() {{
        load(this.getClass.getResourceAsStream("/producer.properties"))
      }})

    val persistedEvent = producer.send(new ProducerRecord("streamWithTwoEventTypes", event1.toJSON(event1)))

    assert(persistedEvent.get().offset() == 0)
    assert(persistedEvent.get().checksum() != 0)

    val kafkaConsumer = new KafkaEventConsumer[Event1, Event2](List("streamWithTwoEventTypes")) {
      addConfiguration(new Properties() {{
          put("group.id", "consumers_testEventsGroup")
          put("client.id", "testKafkaEventConsumer1")
          put("auto.offset.reset", "earliest")
        }}).subscribeEventsInStream(List(classOf[Event1], classOf[Event2]))
        .setEventHandler(new TestTwoEventHandler)

      assert(getConfiguration.getProperty("group.id") == "consumers_testEventsGroup")
      assert(getConfiguration.getProperty("client.id") == "testKafkaEventConsumer1")
    }

    assert(kafkaConsumer.getExistingEventStreams() == List("streamWithTwoEventTypes"))

    val events = kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 1)
  }

  test("given event2, one of two event types in event-stream, consumes it") {

    val event1 = Event2(0l, 0l, classOf[Event2].getName, new Date(), field1 = "event2-data")

    val producer = new KafkaProducer[String, String](new Properties() {{
      load(this.getClass.getResourceAsStream("/producer.properties"))
    }})

    val persistedEvent = producer.send(new ProducerRecord("streamWithTwoEventTypes", event1.toJSON(event1)))

    assert(persistedEvent.get().offset() == 0)
    assert(persistedEvent.get().checksum() != 0)

    val kafkaConsumer = new KafkaEventConsumer[Event1, Event2](List("streamWithTwoEventTypes")) {
      addConfiguration(new Properties() {{
        put("group.id", "consumers_testEventsGroup")
        put("client.id", "testKafkaEventConsumer2")
        put("auto.offset.reset", "earliest")
      }}).subscribeEventsInStream(List(classOf[Event1], classOf[Event2]))
        .setEventHandler(new TestTwoEventHandler)

      assert(getConfiguration.getProperty("group.id") == "consumers_testEventsGroup")
      assert(getConfiguration.getProperty("client.id") == "testKafkaEventConsumer2")
    }

    assert(kafkaConsumer.getExistingEventStreams() == List("streamWithTwoEventTypes"))

    val events = kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 1)
  }

  test("given 2 events in the event-store, consumes events and updates the offset") {

    val kafkaConsumer = new KafkaEventConsumer[Event1, Event2](List("someEventStream")) {
      addConfiguration(new Properties() {{
          put("group.id", "consumers_testEventsGroup")
          put("client.id", "testKafkaEventConsumer3")
          put("auto.offset.reset", "latest")
        }}).subscribeEventsInStream(List(classOf[Event1], classOf[Event2]))
        .setEventHandler(new TestTwoEventHandler)

      assert(getConfiguration.getProperty("group.id") == "consumers_testEventsGroup")
      assert(getConfiguration.getProperty("client.id") == "testKafkaEventConsumer3")
    }
    kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 0)
    assert(kafkaConsumer.getExistingEventStreams() == List("someEventStream", "__consumer_offsets"))

    val producer = new KafkaProducer[String, String](new Properties() {{
        load(this.getClass.getResourceAsStream("/producer.properties"))
      }})

    val persistedEvent1 = producer.send(new ProducerRecord("someEventStream",
      Event1(0l, 0l, classOf[Event1].getName, new Date(), "{event data2}").toString))
    assert(persistedEvent1.get().offset() == 0)
    assert(persistedEvent1.get().checksum() != 0)

    assert(kafkaConsumer.getConsumerPosition == 0)
    val events1 = kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 1)

    val persistedEvent2 = producer.send(new ProducerRecord("someEventStream",
      Event2(0l, 0l, classOf[Event2].getName, new Date(), "{event data3}").toString))

    assert(persistedEvent2.get().offset() == 1)
    assert(persistedEvent2.get().checksum() != 0)

    assert(kafkaConsumer.getExistingEventStreams() == List("someEventStream", "__consumer_offsets"))

    val events2 = kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 2)
  }
}


@JsonIgnoreProperties(Array("eventOffset", "eventHashValue"))
case class Event1(eventOffset: Long, eventHashValue: Long, eventType: String,
                  createdDate: Date, field1: String) extends BaseEvent {

  def this() {
    this(0, 0, "", new Date(), "")
  }

  override def toString: String = toJSON(this.copy())

  override def copyy(eventOffset: Long, eventHashValue: Long): BaseEvent = {
    this.copy(eventOffset = eventOffset, eventHashValue = eventHashValue)
  }
}

@JsonIgnoreProperties(Array("eventOffset", "eventHashValue"))
case class Event2(eventOffset: Long, eventHashValue: Long, eventType: String,
                  createdDate: Date, field1: String) extends BaseEvent {

  def this() {
    this(0, 0, "", new Date(), "")
  }

  override def toString: String = toJSON(this.copy())

  override def copyy(eventOffset: Long, eventHashValue: Long): BaseEvent = {
    this.copy(eventOffset = eventOffset, eventHashValue = eventHashValue)
  }
}
