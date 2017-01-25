package consumer

import java.util.Properties

import event.BaseEvent

/**
  * Created by prayagupd
  * on 1/15/17.
  */
trait EventConsumer {
  def getExistingEventStreams() : List[String]
  def consumeAll()
  def getConfiguration: Properties
  def getConsumerPosition: Long
}

trait SingleEventConsumer[E <: BaseEvent] extends EventConsumer {
  def addConfiguration(key: String, value: String): SingleEventConsumer[E]
  def addConfiguration(properties: Properties): SingleEventConsumer[E]
  def setEventHandler(eventHandler: EventHandler[E]): SingleEventConsumer[E]
  def subscribeEventsInStream(eventTypes: Class[E]) : SingleEventConsumer[E]
  def subscribePartitions(partition: Int) : SingleEventConsumer[E]
}
