package producer

import event.BaseEvent

/**
  * Created by prayagupd
  * on 1/13/17.
  */

trait EventPublisher {
  def publish(event: BaseEvent): BaseEvent
}
