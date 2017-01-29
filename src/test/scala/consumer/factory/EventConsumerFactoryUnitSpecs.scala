package consumer.factory

import java.util.Date

import consumer.kafka.KafkaEventConsumer
import event.BaseEvent
import org.scalatest.FunSuite

/**
  * Created by prayagupd
  * on 1/28/17.
  */

case class Test(eventOffset: Long, eventHashValue: Long, eventType: String,
                createdDate: Date, field1: String) extends BaseEvent {
  override def copyy(eventOffset: Long, eventHashValue: Long): BaseEvent = null
}

class EventConsumerFactoryUnitSpecs extends FunSuite {
  test("when driver is Kafka, creates an instance Kafka consumer") {
    val factory = new EventConsumerFactory[Test]
    val consumer = factory.create(List("some stream"))
    assert(consumer.isInstanceOf[KafkaEventConsumer[Test]])
  }
}
