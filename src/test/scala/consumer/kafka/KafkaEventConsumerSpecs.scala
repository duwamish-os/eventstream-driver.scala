package consumer.kafka

import java.util.{Date, Properties}

import TestHappenedEvent
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by prayagupd
  * on 1/14/17.
  */

class TestEventHandler extends EventHandler[TestHappenedEvent] {

  override def onEvent(event: TestHappenedEvent): Unit = {

    Thread.sleep(10000)
    println(s"event = ${event} => ${new Date()}")
  }
}

class KafkaEventConsumerSpecs extends FunSuite with BeforeAndAfterEach {

  implicit val streamingConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  val kafkaConsumer = new AbstractKafkaEventConsumer[TestHappenedEvent] {
    addConfiguration(new Properties() {{
        put("group.id", "consumers_testEventsGroup")
        put("client.id", "testKafkaEventConsumer")
        put("auto.offset.reset", "earliest")
      }})
      .subscribeEvents(classOf[TestHappenedEvent])
      .setEventHandler(new TestEventHandler)

    assert(getConfiguration.getProperty("group.id") == "consumers_testEventsGroup")
    assert(getConfiguration.getProperty("client.id") == "testKafkaEventConsumer")
  }

  override protected def beforeEach(): Unit = {
    EmbeddedKafka.start()
  }

  override protected def afterEach(): Unit = {
    EmbeddedKafka.stop()
  }

  test("given an event in the event-store, consumes an event") {

    val event = TestHappenedEvent(0l, 0l, classOf[TestHappenedEvent].getSimpleName, new Date(), "data")

    val config = new Properties() {
      {
        load(this.getClass.getResourceAsStream("/producer.properties"))
      }
    }
    val producer = new KafkaProducer[String, String](config)

    val persistedEvent = producer.send(new ProducerRecord(event.getClass.getSimpleName, event.toString))

    assert(persistedEvent.get().offset() == 0)
    assert(persistedEvent.get().checksum() != 0)

    assert(kafkaConsumer.listEventTypesInStream() == List(classOf[TestHappenedEvent].getSimpleName))

    val events = kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 1)
  }

  test("given 2 events in the event-store, consumes events and updates the offset") {

    val event = TestHappenedEvent(0l, 0l, classOf[TestHappenedEvent].getSimpleName, new Date(), "{data1}")
    kafkaConsumer.addConfiguration("auto.offset.reset", "latest")
    kafkaConsumer.consumeAll()

    val config = new Properties() {{
        load(this.getClass.getResourceAsStream("/producer.properties"))
     }}
    val producer = new KafkaProducer[String, String](config)

    val persistedEvent = producer.send(new ProducerRecord(event.getClass.getSimpleName, event.toString))
    assert(persistedEvent.get().offset() == 0)
    assert(persistedEvent.get().checksum() != 0)

    val persistedEvent2 = producer.send(new ProducerRecord(event.getClass.getSimpleName,
      TestHappenedEvent(0l, 0l, classOf[TestHappenedEvent].getSimpleName, new Date(), "{data2}").toString))
    assert(persistedEvent2.get().offset() == 1)
    assert(persistedEvent2.get().checksum() != 0)

    assert(kafkaConsumer.listEventTypesInStream() == List(classOf[TestHappenedEvent].getSimpleName, "__consumer_offsets"))

    val events1 = kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 1)

    val events2 = kafkaConsumer.consumeAll()
    assert(kafkaConsumer.getConsumerPosition == 2)
  }
}
