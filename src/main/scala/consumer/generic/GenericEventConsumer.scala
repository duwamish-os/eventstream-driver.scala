package consumer.generic

import java.util.Properties

import consumer.factory.EventConsumerFactory
import consumer.{EventHandler, SingleEventConsumer}
import event.BaseEvent

/**
  * Created by prayagupd
  * on 1/23/17.
  */

class GenericEventConsumer[E <: BaseEvent](streams: List[String]) extends SingleEventConsumer[E]{

  val consumerFactory = new EventConsumerFactory[E]

  val consumer = consumerFactory.create(streams)

  override def addConfiguration(key: String, value: String): SingleEventConsumer[E] = consumer.addConfiguration(key, value)

  override def addConfiguration(properties: Properties): SingleEventConsumer[E] = consumer.addConfiguration(properties)

  override def setEventHandler(eventHandler: EventHandler[E]): SingleEventConsumer[E] = consumer.setEventHandler(eventHandler)

  override def subscribeEventsInStream(eventTypes: Class[E]): SingleEventConsumer[E] = consumer.subscribeEventsInStream(eventTypes)

  override def subscribePartitions(partition: Int): SingleEventConsumer[E] = consumer.subscribePartitions(partition)

  override def getExistingEventStreams(): List[String] = consumer.getExistingEventStreams()

  override def consumeAll(): Unit = consumer.consumeAll()

  override def getConfiguration: Properties = consumer.getConfiguration

  override def getConsumerPosition: Long = consumer.getConsumerPosition
}
