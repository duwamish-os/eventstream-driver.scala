package consumer

import java.util.Properties

import offset.PartitionOffset
import org.apache.kafka.clients.consumer.ConsumerRecord
import producer.{AbstractEvent, BaseEvent}

/**
  * Created by prayagupd
  * on 1/15/17.
  */

trait EventConsumer[E <: BaseEvent] {
  def setEventType(eventType: Class[E]) : EventConsumer[E]
  def setEventHandler(eventHandler: EventHandler[E]): EventConsumer[E]
  def subscribeEvents(eventTypes: List[String]) : EventConsumer[E]

  def listEventTypesInStream() : List[String]
  def consumeAll()
  def getConfiguration() : Properties
}
