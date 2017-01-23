package consumer

import java.util.Date
import java.util.zip.CRC32

import consumer.kafka.{EventHandler, TestHappenedEvent}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import producer.GenericEventPublisher

/**
  * Created by prayagupd
  * on 1/23/17.
  */

class GenericEventConsumerSpecs extends FunSuite with BeforeAndAfterEach {

  implicit val streamingConfig = new EmbeddedKafkaConfig(zooKeeperPort = 2181, kafkaPort = 9092)

  val eventProducer = new GenericEventPublisher

  val genericEventConsumer = new GenericEventConsumer[TestHappenedEvent]
    .addConfiguration("group.id", "some_consumer_group")
    .addConfiguration("client.id", "genericEventConsumerInstance")
    .addConfiguration("auto.offset.reset", "earliest")
    .subscribeEvents(classOf[TestHappenedEvent])
    .setEventHandler(new EventHandler[TestHappenedEvent] {
      override def onEvent(event: TestHappenedEvent): Unit = println(s"processing $event")
    })

  override protected def beforeEach(): Unit = EmbeddedKafka.start()
  override protected def afterEach(): Unit = EmbeddedKafka.stop()

  test("given events in the stream, consumes each event and updates the consumer_offset") {
    val event = TestHappenedEvent(eventOffset=0, eventHashValue = 0, eventType = classOf[TestHappenedEvent].getSimpleName,
      createdDate = new Date(2017, 10, 28), field1 = "value1")

    val persistedEvent = eventProducer.publish(event)
    assert(persistedEvent.eventOffset == 0)
    assert(persistedEvent.eventHashValue > 0)

    genericEventConsumer.consumeAll()

    assert(genericEventConsumer.getConsumerPosition == 1)
  }
}
