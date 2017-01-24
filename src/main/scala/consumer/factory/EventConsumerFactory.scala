package consumer.factory

import com.typesafe.config.ConfigFactory
import consumer.EventConsumer
import consumer.kafka.AbstractKafkaEventConsumer
import event.BaseEvent

/**
  * Created by prayagupd
  * on 1/23/17.
  */
class EventConsumerFactory[E <: BaseEvent] {

  def create (): EventConsumer[E] = {
    ConfigFactory.load("streaming.conf").getString("streaming.driver") match {
      case "Kafka" => new AbstractKafkaEventConsumer[E] {}
      case _ => throw new NoSuchElementException("No such driver found.")
    }
  }
}
