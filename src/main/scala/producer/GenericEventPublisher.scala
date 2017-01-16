package producer

/**
  * Created by prayagupd
  * on 1/15/17.
  */

class GenericEventPublisher extends EventPublisher {

  val eventPublisherFactory = new EventPublisherFactory
  var eventPublisher = eventPublisherFactory.create()

  override def publish(event: BaseEvent): BaseEvent = {
    eventPublisher.publish(event)
  }
}
