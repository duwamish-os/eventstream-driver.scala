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
    load(this.getClass.getResourceAsStream("/consumer.properties"))
  }}

  var consumer : KafkaConsumer[String, String] = null

  override def consumeEvent(eventRecord: ConsumerRecord[String, String])

  override def consumeAll() = {
    val events = consumer.poll(1000)
    for (e <- events) {
      consumeEvent(e)
    }
  }

  override def subscribeEvents(eventTypes: List[String]): EventConsumer[E] = {
    consumer = new KafkaConsumer[String, String](config)
    consumer.subscribe(eventTypes)
    this
  }

  def addConfiguration(key: String, value: String): EventConsumer[E] = {
    config.put(key, value)
    this
  }

  def addConfiguration(properties: Properties): EventConsumer[E] = {
    config.putAll(properties)
    this
  }

  override def listEventTypes(): List[String] = {
    consumer.listTopics().map(_._1).toList
  }
}
