package consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import producer.BaseEvent

/**
  * Created by prayagupd
  * on 1/15/17.
  */

trait EventConsumer[E >: BaseEvent] {
  def consume(consumerRecord: ConsumerRecord[String, String])
  def consumeAll()
  def subscribeEvents(eventTypes: List[String]) : EventConsumer[E]
}
