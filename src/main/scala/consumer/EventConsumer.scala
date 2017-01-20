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
  def setEventHandler(eventHandler: EventHandler[E]): EventConsumer[E]
  def subscribeEvents(eventTypes: Class[E]) : EventConsumer[E]

  def listEventTypesInStream() : List[String]
  def consumeAll()
  def getConfiguration: Properties
  def getConsumerPosition: Long
}
