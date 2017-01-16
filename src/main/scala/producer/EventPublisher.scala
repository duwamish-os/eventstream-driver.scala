package producer

import java.util.Date

/**
  * Created by prayagupd
  * on 1/13/17.
  */

trait BaseEvent {
  def eventOffset : Long
  def hashValue : Long
  def created : Date
}

case class AbstractEvent(eventOffset: Long, hashValue: Long, created: Date) extends BaseEvent

trait EventPublisher {
  def publish(event: BaseEvent) : BaseEvent
}
