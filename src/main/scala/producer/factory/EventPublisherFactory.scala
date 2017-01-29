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
    streamingConfig.getString("streaming.producer.driver") match {
      case "Kafka" => new KafkaEventPublisher(stream) //FIXME has to be Class.forName so that does not need to redeploy
      case _ => throw new Exception(s"${streamingConfig.getString("streaming.producer.driver")}, Driver not found.")
    }
  }
}
