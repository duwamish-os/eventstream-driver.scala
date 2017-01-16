package producer

import java.util.Date

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.FunSuite

/**
  * Created by prayagupd
  * on 1/15/17.
  */
case class TestEvent(eventOffset: Long, hashValue: Long, created: Date) extends BaseEvent

class GenericEventPublisherIntegrationSpecs extends FunSuite {

  val genericEventPublisher = new GenericEventPublisher

  test("publishes an event based on streaming-conf") {

    implicit val streamingConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

    EmbeddedKafka.start()

    val event = TestEvent(0, 0, new Date())

    val persistedEvent1 = genericEventPublisher.publish(TestEvent(0, 0, new Date()))
    assert(persistedEvent1.eventOffset == 0)

    val persistedEvent2 = genericEventPublisher.publish(TestEvent(1, 2, new Date()))
    assert(persistedEvent2.eventOffset == 1)

    EmbeddedKafka.stop()
  }
}
