package consumer.factory

import com.typesafe.config.ConfigFactory
import consumer.SingleEventConsumer
import consumer.kafka.KafkaEventConsumer
import consumer.kafka.two.{KafkaEventConsumer => KafkaEventConsumer2, TwoEventConsumer}
import event.BaseEvent

/**
  * Created by prayagupd
  * on 1/23/17.
  */
class EventConsumerFactory {

  def create [E1 <: BaseEvent](streams: List[String]): SingleEventConsumer[E1] = {
    ConfigFactory.load("streaming.conf").getString("streaming.consumer.driver") match {
      case "Kafka" => new KafkaEventConsumer[E1](streams) {}
      case _ => throw new NoSuchElementException(s"${ConfigFactory.load("streaming.conf")
        .getString("streaming.consumer.driver")}, No such driver found.")
    }
  }

  def create [E1 <: BaseEvent, E2 <: BaseEvent](streams: List[String]): TwoEventConsumer[E1, E2] = {
    ConfigFactory.load("streaming.conf").getString("streaming.consumer.driver") match {
      case "Kafka" => new KafkaEventConsumer2[E1, E2](streams) {}
      case _ => throw new NoSuchElementException(s"${ConfigFactory.load("streaming.conf")
        .getString("streaming.consumer.driver")}, No such driver found.")
    }
  }
}
