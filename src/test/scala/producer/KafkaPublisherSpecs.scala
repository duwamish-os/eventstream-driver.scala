package producer

import java.util.Date
import java.util.concurrent.{Future, TimeUnit}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito
import org.scalatest.FunSuite

/**
  * Created by prayagupd
  * on 1/14/17.
  */

class KafkaPublisherSpecs extends FunSuite {
  val kafkaPublisher = new KafkaEventPublisher
  kafkaPublisher.producer = Mockito.mock(classOf[KafkaProducer[String, String]])

  case class TestEvent(eventOffset: String, hashValue: Long, created: Date) extends BaseEvent


  test("produces a record and returns event with checksum") {
    val event = new TestEvent("001", 12l, new Date())

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

    Mockito.when(kafkaPublisher.producer.send(new ProducerRecord[String, String](event.getClass.getSimpleName, event.toString))) thenReturn mockMetadata

    val returndEvent = kafkaPublisher.publish(event)

    Mockito.verify(kafkaPublisher.producer).send(new ProducerRecord[String, String](event.getClass.getSimpleName, event.toString))

    assert(returndEvent.hashValue == 100l)
  }
}
