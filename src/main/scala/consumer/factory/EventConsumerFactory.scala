package consumer.factory

import com.typesafe.config.ConfigFactory
import consumer.SingleEventConsumer
import consumer.kafka.KafkaEventConsumer
import event.BaseEvent

/**
  * Created by prayagupd
  * on 1/23/17.
  */
class EventConsumerFactory[E <: BaseEvent] {

  def create (streams: List[String]): SingleEventConsumer[E] = {
    ConfigFactory.load("streaming.conf").getString("streaming.consumer.driver") match {
      case "Kafka" => new KafkaEventConsumer[E](streams) {}
      case _ => throw new NoSuchElementException(s"${ConfigFactory.load("streaming.conf")
        .getString("streaming.consumer.driver")}, No such driver found.")
    }
  }
}
