package producer.kafka

import java.util.{Properties, concurrent}

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
    val metadata : concurrent.Future[RecordMetadata] =
      producer.send(new ProducerRecord[String, String](event.getClass.getSimpleName, event.toString))
    AbstractEvent(metadata.get().offset(), metadata.get().checksum(), event.created)
  }
}
