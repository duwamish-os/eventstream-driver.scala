package consumer

import java.lang.reflect.Method
import java.util
import java.util.{Date, Properties}

import offset.{Offset, PartitionOffset}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import producer.BaseEvent

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by prayagupd
  * on 1/15/17.
  */

trait EventHandler[E <: BaseEvent] {
  def onEvent(event: E)
}

abstract class AbstractKafkaEventConsumer[E <: BaseEvent] extends EventConsumer[E] {

  var eventType: Class[E] = _

  var eventHandler: EventHandler[E] = _

  val config = new Properties() {
    {
      load(this.getClass.getResourceAsStream("/consumer.properties"))
    }
  }

  var consumer: KafkaConsumer[String, String] = null

  override def consumeAll() = {
    val events = consumer.poll(1000)
    for (eventRecord <- events) {

      val method: Method = eventType.getMethod("fromPayload", classOf[String])
      val s = method.invoke(eventType.newInstance(), eventRecord.value())
      eventHandler.onEvent(s.asInstanceOf[E])
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

  override def listEventTypesInStream(): List[String] = {
    consumer.listTopics().map(_._1).toList
  }

  override def setEventType(eventType: Class[E]): EventConsumer[E] = {
    this.eventType = eventType
    this
  }

  def setEventHandler(eventHandler: EventHandler[E]): EventConsumer[E] = {
    this.eventHandler = eventHandler
    this
  }

  override def getConfiguration(): Properties = config

}
