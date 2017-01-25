package producer.factory

import com.typesafe.config.ConfigFactory
import producer.EventPublisher
import producer.kafka.KafkaEventPublisher

/**
  * Created by prayagupd
  * on 1/15/17.
  */

class EventPublisherFactory {

  val streamingConfig = ConfigFactory.load("streaming.conf")

  def create(stream: String) : EventPublisher = {
    streamingConfig.getString("streaming.driver") match {
      case "Kafka" => new KafkaEventPublisher(stream)
      case _ => null
    }
  }
}
