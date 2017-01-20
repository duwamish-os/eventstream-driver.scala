package consumer

import java.util.Date

import producer.BaseEvent

/**
  * Created by prayagupd
  * on 1/19/17.
  */

case class TestHappenedEvent(eventOffset: Long, hashValue: Long, eventType: String, created: Date, testField: String) extends BaseEvent {

  def this() {
    this(0, 0, "", new Date(), "")
  }

  override def fromPayload(payload: String): BaseEvent = {
    TestHappenedEvent(0, 0, payload, new Date(), "test field")
  }
}
