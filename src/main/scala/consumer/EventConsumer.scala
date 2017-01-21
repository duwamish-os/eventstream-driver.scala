package consumer

import java.util.Properties

import consumer.kafka.EventHandler
import producer.BaseEvent

/**
  * Created by prayagupd
  * on 1/15/17.
  */

trait EventConsumer[E <: BaseEvent] {
  def addConfiguration(key: String, value: String): EventConsumer[E]
  def addConfiguration(properties: Properties): EventConsumer[E]
  def setEventHandler(eventHandler: EventHandler[E]): EventConsumer[E]
  def subscribeEvents(eventTypes: Class[E]) : EventConsumer[E]
  def subscribePartitions(partition: Int) : EventConsumer[E]

  def listEventTypesInStream() : List[String]
  def consumeAll()
  def getConfiguration: Properties
  def getConsumerPosition: Long
}
