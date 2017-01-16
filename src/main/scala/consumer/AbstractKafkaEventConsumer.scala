package consumer

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import producer.BaseEvent

import scala.collection.JavaConversions._

/**
  * Created by prayagupd
  * on 1/15/17.
  */

abstract class AbstractKafkaEventConsumer[E >: BaseEvent] extends EventConsumer[E] {

  val config = new Properties() {{
    put("bootstrap.servers", "localhost:9092") //streaming.config
    put("group.id", s"consumer_group_${this.getClass.getSimpleName}")
    put("enable.auto.commit", "true")
    put("auto.commit.interval.ms", "1000")
    put("session.timeout.ms", "30000")
    put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  }}

  val consumer = new KafkaConsumer[String, String](config)

  override def consume(eventRecord: ConsumerRecord[String, String])

  override def consumeAll() = {

    val events = consumer.poll(1000)
    for (e <- events) {
      consume(e)
    }
  }

  override def subscribeEvents(eventTypes: List[String]): Unit = {
    consumer.subscribe(eventTypes)
  }

}
