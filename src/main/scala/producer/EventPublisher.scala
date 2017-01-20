package producer

import java.util.Date

/**
  * Created by prayagupd
  * on 1/13/17.
  */

trait BaseEvent {
  def eventOffset : Long
  def hashValue : Long
  def eventType: String
  def created : Date
  def fromPayload(payload: String) : BaseEvent
}

case class AbstractEvent(eventOffset: Long, hashValue: Long, eventType: String, created: Date) extends BaseEvent {

  override def fromPayload(payload: String): BaseEvent = {
    AbstractEvent(0, 0, payload, new Date())
  }
}

trait EventPublisher {
  def publish(event: BaseEvent) : BaseEvent
}
