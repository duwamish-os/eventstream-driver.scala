package consumer.kafka

import java.lang.reflect.Method
import java.util.{Collections, Properties}

import consumer.{SingleEventConsumer, EventHandler}
import event.{BaseEvent, EventOffsetAndHashValue}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

/**
  * Created by prayagupd
  * on 1/15/17.
  */

abstract class AbstractKafkaSingleEventConsumer[E <: BaseEvent](streams: List[String]) extends SingleEventConsumer[E] {

  var eventTypePartition: Int = 0
  var subscribedEventType: Class[E] = _

  var eventHandler: EventHandler[E] = _

  val config = new Properties() {
    {
      load(this.getClass.getResourceAsStream("/consumer.properties"))
    }
  }

  var nativeConsumer: KafkaConsumer[String, String] = null

  override def consumeAll() = {
    val events = nativeConsumer.poll(1000)

    println(s"Events consumed at once = ${events.size}")

    for (eventRecord <- events) {

      println("=================================== consumer offset ===============================|")
      println(s"================================== $getConsumerPosition ==========================|")
      println("=================================== consumer offset ===============================|")

      val method: Method = subscribedEventType.getMethod("fromPayload", classOf[EventOffsetAndHashValue],
        classOf[String], subscribedEventType.getClass)
      val s = method.invoke(subscribedEventType.newInstance(),
        EventOffsetAndHashValue(eventRecord.offset(), eventRecord.checksum()), eventRecord.value(), subscribedEventType)
      eventHandler.onEvent(s.asInstanceOf[E])
    }
  }

  override def subscribeEventsInStream(eventTypes: Class[E]): SingleEventConsumer[E] = {
    this.subscribedEventType = eventTypes
    nativeConsumer = new KafkaConsumer[String, String](config)
    nativeConsumer.subscribe(streams)
    this
  }

  override def subscribePartitions(partition: Int): SingleEventConsumer[E] = {
    this.eventTypePartition = partition
    this
  }

  override def addConfiguration(key: String, value: String): SingleEventConsumer[E] = {
    config.put(key, value)
    this
  }

  override def addConfiguration(properties: Properties): SingleEventConsumer[E] = {
    config.putAll(properties)
    this
  }

  override def getExistingEventStreams(): List[String] = {
    nativeConsumer.listTopics().map(_._1).toList
  }

  def setEventHandler(eventHandler: EventHandler[E]): SingleEventConsumer[E] = {
    this.eventHandler = eventHandler
    this
  }

  override def getConfiguration: Properties = config

  override def getConsumerPosition: Long = {
    nativeConsumer.position(new TopicPartition(streams.head, eventTypePartition))
  }
}
