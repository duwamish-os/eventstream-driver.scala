package consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import producer.BaseEvent

/**
  * Created by prayagupd
  * on 1/15/17.
  */

trait EventConsumer[E >: BaseEvent] {
  def consumeEvent(consumerRecord: ConsumerRecord[String, String])
  def consumeAll()
  def subscribeEvents(eventTypes: List[String]) : EventConsumer[E]
  def listEventTypes() : List[String]
}
