package producer.factory

import org.scalatest.FunSuite
import producer.kafka.KafkaEventPublisher

/**
  * Created by prayagupd
  * on 1/15/17.
  */

class PublisherFactoryUnitSpecs extends FunSuite {

  val publisherFactory = new EventPublisherFactory

  test("when config is KafkaDriver, returns a Kafka Publisher") {
    val publisher = publisherFactory.create("whatever stream")
    assert(publisher.isInstanceOf[KafkaEventPublisher])
  }
}
