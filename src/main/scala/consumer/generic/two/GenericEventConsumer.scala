package consumer.generic.two

import java.util.Properties

import consumer.TwoEventsHandler
import consumer.factory.EventConsumerFactory
import consumer.kafka.two.TwoEventConsumer
import event.BaseEvent

/**
  * Created by prayagupd
  * on 1/23/17.
  */

class GenericEventConsumer[E1 <: BaseEvent, E2 <: BaseEvent](streams: List[String]) extends TwoEventConsumer[E1, E2]{

  val consumerFactory = new EventConsumerFactory

  val consumer = consumerFactory.create[E1, E2](streams)

  override def addConfiguration(key: String, value: String): TwoEventConsumer[E1, E2] =
    consumer.addConfiguration(key, value)

  override def addConfiguration(properties: Properties): TwoEventConsumer[E1, E2] =
    consumer.addConfiguration(properties)

  override def setEventHandler(eventHandler: TwoEventsHandler[E1, E2]): TwoEventConsumer[E1, E2] =
    consumer.setEventHandler(eventHandler)

  override def subscribeEventsInStream(eventTypes: List[Class[_ <: BaseEvent]]): TwoEventConsumer[E1, E2] =
    consumer.subscribeEventsInStream(eventTypes)

  override def subscribePartitions(partition: Int): TwoEventConsumer[E1, E2] = consumer.subscribePartitions(partition)

  override def getExistingEventStreams(): List[String] = consumer.getExistingEventStreams()

  override def consumeAll(): Unit = consumer.consumeAll()

  override def getConfiguration: Properties = consumer.getConfiguration

  override def getConsumerPosition: Long = consumer.getConsumerPosition
}
