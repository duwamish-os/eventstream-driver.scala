package producer

import event.BaseEvent
import producer.factory.EventPublisherFactory

/**
  * Created by prayagupd
  * on 1/15/17.
  */

class GenericEventPublisher(stream: String) extends EventPublisher {

  val eventPublisherFactory = new EventPublisherFactory
  var eventPublisher = eventPublisherFactory.create(stream)

  override def publish(event: BaseEvent): BaseEvent = {
    eventPublisher.publish(event)
  }
}
