package consumer.factory

import com.typesafe.config.ConfigFactory
import consumer.SingleEventConsumer
import consumer.kafka.AbstractKafkaSingleEventConsumer
import event.BaseEvent

/**
  * Created by prayagupd
  * on 1/23/17.
  */
class EventConsumerFactory[E <: BaseEvent] {

  def create (streams: List[String]): SingleEventConsumer[E] = {
    ConfigFactory.load("streaming.conf").getString("streaming.driver") match {
      case "Kafka" => new AbstractKafkaSingleEventConsumer[E](streams) {}
      case _ => throw new NoSuchElementException("No such driver found.")
    }
  }
}
