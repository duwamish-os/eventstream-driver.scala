package producer

import java.util.Date

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.FunSuite

/**
  * Created by prayagupd
  * on 1/15/17.
  */
case class ItemSoldEvent(eventOffset: Long, hashValue: Long, eventType: String, created: Date) extends BaseEvent {
  override def fromPayload(payload: String): BaseEvent = null
}

class GenericEventPublisherIntegrationSpecs extends FunSuite {

  val genericEventPublisher = new GenericEventPublisher

  test("publishes an event based on streaming-conf") {

    implicit val streamingConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

    EmbeddedKafka.start()

    val event = ItemSoldEvent(0, 0, classOf[ItemSoldEvent].getSimpleName, new Date())

    val persistedEvent1 = genericEventPublisher.publish(ItemSoldEvent(0, 0, classOf[ItemSoldEvent].getSimpleName, new Date()))
    assert(persistedEvent1.eventOffset == 0)

    val persistedEvent2 = genericEventPublisher.publish(ItemSoldEvent(1, 2, classOf[ItemSoldEvent].getSimpleName, new Date()))
    assert(persistedEvent2.eventOffset == 1)

    EmbeddedKafka.stop()
  }
}
