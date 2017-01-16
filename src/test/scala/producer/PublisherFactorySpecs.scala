package producer

import org.scalatest.FunSuite
import producer.kafka.KafkaEventPublisher

/**
  * Created by prayagupd
  * on 1/15/17.
  */

class PublisherFactorySpecs extends FunSuite {

  val publisherFactory = new EventPublisherFactory

  test("when config is KafkaDriver, returns a Kafka Publisher") {
    val publisher = publisherFactory.create()
    assert(publisher.isInstanceOf[KafkaEventPublisher])
  }
}
