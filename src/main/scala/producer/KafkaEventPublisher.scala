package producer

import java.util.Properties
import java.util.concurrent

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * Created by prayagupd
  * on 1/14/17.
  */

class KafkaEventPublisher extends EventProducer {

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
