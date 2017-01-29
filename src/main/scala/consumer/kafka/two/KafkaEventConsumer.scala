package consumer.kafka.two

import java.util.Properties

import consumer.{EventConsumer, TwoEventsHandler}
import event.BaseEvent
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.json.JSONObject

import scala.collection.JavaConversions._

/**
  * Created by prayagupd
  * on 1/25/17.
  */

trait TwoEventConsumer[E1 <: BaseEvent, E2 <: BaseEvent] extends EventConsumer {
  def addConfiguration(key: String, value: String): TwoEventConsumer[E1, E2]
  def addConfiguration(properties: Properties): TwoEventConsumer[E1, E2]
  def setEventHandler(eventHandler: TwoEventsHandler[E1, E2]): TwoEventConsumer[E1, E2]
  def subscribeEventsInStream(eventTypes: List[Class[_ <: BaseEvent]]) : TwoEventConsumer[E1, E2]
  def subscribePartitions(partition: Int) : TwoEventConsumer[E1, E2]
}

abstract class KafkaEventConsumer[E1 <: BaseEvent, E2 <: BaseEvent](streams: List[String]) extends TwoEventConsumer[E1, E2] {

  var eventTypePartition: Int = 0
  var subscribedEventType: List[Class[_ <: BaseEvent]] = _

  var eventHandler: TwoEventsHandler[E1, E2] = _

  val config = new Properties() {{
      load(this.getClass.getResourceAsStream("/consumer.properties"))
    }}

  var nativeConsumer: KafkaConsumer[String, String] = null

  override def consumeAll() = {
    val events = nativeConsumer.poll(1000)
    for (eventRecord <- events) {

      val json = new JSONObject(eventRecord.value())

      //TODO retry x times
      //TODO write to errorStream on any exception, not finding eventType
      val whatIsEventType : Class[_ <: BaseEvent] = Class.forName(json.getString("eventType"))
        .asSubclass(classOf[BaseEvent])

      val actualEventInStream = whatIsEventType.newInstance().fromJSON(eventRecord.value(), whatIsEventType)

      //FIXME stupid way of solving matching the HEAD
      //FIXME drain if other than Event1 or Event2, because stream can have lots of eventTypes,
      //and for Event3, it will throw exception
      whatIsEventType.isAssignableFrom(subscribedEventType.head) match {
        case true => eventHandler.onEventA(actualEventInStream.asInstanceOf[E1])
        case false => eventHandler.onEventB(actualEventInStream.asInstanceOf[E2])
      }
    }
  }

  override def subscribeEventsInStream(eventTypes: List[Class[_ <: BaseEvent]]): TwoEventConsumer[E1, E2] = {
    this.subscribedEventType = eventTypes
    nativeConsumer = new KafkaConsumer[String, String](config)
    nativeConsumer.subscribe(streams)
    this
  }

  override def subscribePartitions(partition: Int): TwoEventConsumer[E1, E2] = {
    this.eventTypePartition = partition
    this
  }

  override def addConfiguration(key: String, value: String): TwoEventConsumer[E1, E2] = {
    config.put(key, value)
    this
  }

  override def addConfiguration(properties: Properties): TwoEventConsumer[E1, E2] = {
    config.putAll(properties)
    this
  }

  override def getExistingEventStreams(): List[String] = nativeConsumer.listTopics().map(_._1).toList

  def setEventHandler(eventHandler: TwoEventsHandler[E1, E2]): TwoEventConsumer[E1, E2] = {
    this.eventHandler = eventHandler
    this
  }

  override def getConfiguration: Properties = config

  override def getConsumerPosition: Long = nativeConsumer.position(new TopicPartition(streams.head, eventTypePartition))
}
