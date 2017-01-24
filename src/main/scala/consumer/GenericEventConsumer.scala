package consumer

import java.util.Properties

import consumer.factory.EventConsumerFactory
import event.BaseEvent

/**
  * Created by prayagupd
  * on 1/23/17.
  */

class GenericEventConsumer[E <: BaseEvent] extends EventConsumer[E]{

  val consumerFactory = new EventConsumerFactory[E]

  val consumer = consumerFactory.create()

  override def addConfiguration(key: String, value: String): EventConsumer[E] = consumer.addConfiguration(key, value)

  override def addConfiguration(properties: Properties): EventConsumer[E] = consumer.addConfiguration(properties)

  override def setEventHandler(eventHandler: EventHandler[E]): EventConsumer[E] = consumer.setEventHandler(eventHandler)

  override def subscribeEvents(eventTypes: Class[E]): EventConsumer[E] = consumer.subscribeEvents(eventTypes)

  override def subscribePartitions(partition: Int): EventConsumer[E] = consumer.subscribePartitions(partition)

  override def listEventTypesInStream(): List[String] = consumer.listEventTypesInStream()

  override def consumeAll(): Unit = consumer.consumeAll()

  override def getConfiguration: Properties = consumer.getConfiguration

  override def getConsumerPosition: Long = consumer.getConsumerPosition
}
