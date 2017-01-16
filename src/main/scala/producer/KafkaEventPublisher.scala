package producer

import java.util.Properties
import java.util.concurrent

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * Created by prayagupd
  * on 1/14/17.
  */

class KafkaEventPublisher extends EventProducer {

  val config = new Properties() {{
    put("bootstrap.servers", "localhost:9092") //streaming.config
    put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  }}

  var producer = new KafkaProducer[String, String](config)

  override def publish(event: BaseEvent): BaseEvent = {
    val metadata : concurrent.Future[RecordMetadata] =
      producer.send(new ProducerRecord[String, String](event.getClass.getSimpleName, event.toString))
    AbstractEvent(metadata.get().offset(), metadata.get().checksum(), event.created)
  }
}
