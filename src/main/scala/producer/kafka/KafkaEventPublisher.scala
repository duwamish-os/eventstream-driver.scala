package producer.kafka

import java.util.{Date, Properties, concurrent}

import event.{BaseEvent, EventOffsetAndHashValue}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import producer.EventPublisher

/**
  * Created by prayagupd
  * on 1/14/17.
  */

class KafkaEventPublisher extends EventPublisher {

  val config = new Properties(){{
    load(this.getClass.getResourceAsStream("/producer.properties"))
  }}

  var eventProducer = new KafkaProducer[String, String](config)

  override def publish(event: BaseEvent): BaseEvent = {

    val publishedMetadata : concurrent.Future[RecordMetadata] =
      eventProducer.send(new ProducerRecord[String, String](event.getClass.getSimpleName, event.toJSON(event)))

    //FIXME .copy instead
    new BaseEvent {
      override def createdDate: Date = new Date(publishedMetadata.get().timestamp())

      override def eventOffset: Long = publishedMetadata.get().offset()

      override def eventHashValue: Long = publishedMetadata.get().checksum()

      override def eventType: String = publishedMetadata.get().topic()

      //FIXME make it a direct instance of event
      override def copyy(eventOffset: Long, eventHashValue: Long): BaseEvent = {
        this
      }
    }.asInstanceOf[event.type]

  }
}
