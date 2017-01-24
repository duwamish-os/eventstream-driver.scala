package producer.kafka

import java.util.Date
import java.util.concurrent.{Future, TimeUnit}

import event.{BaseEvent, EventOffsetAndHashValue}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito
import org.scalatest.FunSuite

/**
  * Created by prayagupd
  * on 1/14/17.
  */

class KafkaPublisherUnitSpecs extends FunSuite {
  val kafkaPublisher = new KafkaEventPublisher
  kafkaPublisher.eventProducer = Mockito.mock(classOf[KafkaProducer[String, String]])

  case class InventoryMovedEvent(eventOffset: Long, eventHashValue: Long, eventType: String, createdDate: Date) extends BaseEvent {
    override def copyy(eventOffset: Long, eventHashValue: Long): BaseEvent = {
      this.copy(eventOffset = eventOffset, eventHashValue = eventHashValue)
    }
  }

  test("produces a record and returns event with checksum") {
    val event = InventoryMovedEvent(1l, 12l, classOf[InventoryMovedEvent].getSimpleName, new Date())

    val mockMetadata = new Future[RecordMetadata] {
      override def isCancelled: Boolean = false

      override def get(): RecordMetadata = {
        return new RecordMetadata(new TopicPartition("", 0), 0, 0, 1223, 100l, 0, 0)
      }

      override def get(timeout: Long, unit: TimeUnit): RecordMetadata = {
        get()
      }

      override def cancel(mayInterruptIfRunning: Boolean): Boolean = false

      override def isDone: Boolean = false
    }

    Mockito.when(kafkaPublisher.eventProducer
      .send(new ProducerRecord[String, String](event.getClass.getSimpleName, event.toJSON(event)))) thenReturn mockMetadata

    val returndEvent = kafkaPublisher.publish(event)

    Mockito.verify(kafkaPublisher.eventProducer).send(
      new ProducerRecord[String, String](event.getClass.getSimpleName, event.toJSON(event)))

    assert(returndEvent.eventHashValue == 100l)
  }
}
