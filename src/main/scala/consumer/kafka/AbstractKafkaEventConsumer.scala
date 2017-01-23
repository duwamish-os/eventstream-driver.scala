package consumer.kafka

import java.lang.reflect.Method
import java.util.{Collections, Properties}

import consumer.EventConsumer
import offset.EventOffsetAndHashValue
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import producer.BaseEvent

import scala.collection.JavaConversions._

/**
  * Created by prayagupd
  * on 1/15/17.
  */

trait EventHandler[E <: BaseEvent] {
  def onEvent(event: E)
}

abstract class AbstractKafkaEventConsumer[E <: BaseEvent] extends EventConsumer[E] {

  var eventTypePartition: Int = 0
  var subscribedEventType: Class[E] = _

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

      println("offset =============================================================|")
      println(getConsumerPosition)
      println("offset =============================================================|")

      val method: Method = subscribedEventType.getMethod("fromPayload", classOf[EventOffsetAndHashValue], classOf[String])
      val s = method.invoke(subscribedEventType.newInstance(),
        EventOffsetAndHashValue(eventRecord.offset(), eventRecord.checksum()), eventRecord.value())
      eventHandler.onEvent(s.asInstanceOf[E])
    }
  }

  override def subscribeEvents(eventTypes: Class[E]): EventConsumer[E] = {
    this.subscribedEventType = eventTypes
    consumer = new KafkaConsumer[String, String](config)
    consumer.subscribe(Collections.singletonList(subscribedEventType.getSimpleName))
    this
  }

  override def subscribePartitions(partition: Int): EventConsumer[E] = {
    this.eventTypePartition = partition
    this
  }

  override def addConfiguration(key: String, value: String): EventConsumer[E] = {
    config.put(key, value)
    this
  }

  override def addConfiguration(properties: Properties): EventConsumer[E] = {
    config.putAll(properties)
    this
  }

  override def listEventTypesInStream(): List[String] = {
    consumer.listTopics().map(_._1).toList
  }

  def setEventHandler(eventHandler: EventHandler[E]): EventConsumer[E] = {
    this.eventHandler = eventHandler
    this
  }

  override def getConfiguration: Properties = config

  override def getConsumerPosition: Long = {
    consumer.position(new TopicPartition(subscribedEventType.getSimpleName, eventTypePartition))
  }
}
