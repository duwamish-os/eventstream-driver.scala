package producer

import com.typesafe.config.ConfigFactory
import producer.kafka.KafkaEventPublisher

/**
  * Created by prayagupd
  * on 1/15/17.
  */

class EventPublisherFactory {

  val streamingConfig = ConfigFactory.load("streaming.conf")

  def create() : EventPublisher = {
    streamingConfig.getString("streaming.driver") match {
      case "Kafka" => new KafkaEventPublisher
      case _ => null
    }
  }
}
