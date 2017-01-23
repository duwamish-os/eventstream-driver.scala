package producer.kafka

import java.util.{Date, Properties, concurrent}

import offset.EventOffsetAndHashValue
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import producer.{AbstractEvent, BaseEvent, EventPublisher}

/**
  * Created by prayagupd
  * on 1/14/17.
  */

class KafkaEventPublisher extends EventPublisher {

  val config = new Properties(){{
    load(this.getClass.getResourceAsStream("/producer.properties"))
  }}

  var producer = new KafkaProducer[String, String](config)

  override def publish(event: BaseEvent): BaseEvent = {

    val publishedMetadata : concurrent.Future[RecordMetadata] =
      producer.send(new ProducerRecord[String, String](event.getClass.getSimpleName, event.toString))

    new BaseEvent {
      override def createdDate: Date = new Date(publishedMetadata.get().timestamp())

      override def eventOffset: Long = publishedMetadata.get().offset()

      override def eventHashValue: Long = publishedMetadata.get().checksum()

      override def eventType: String = publishedMetadata.get().topic()

      override def fromPayload(offset: EventOffsetAndHashValue, payload: String): BaseEvent = {null}

      override def toJSON(): String = {null}
    }.asInstanceOf[event.type]

  }
}
