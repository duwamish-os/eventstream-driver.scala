package producer.kafka

import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import event.{BaseEvent, EventOffsetAndHashValue}

/**
  * Created by prayagupd
  * on 1/23/17.
  */

case class SomeObject(field1: String, field2: String)

case class TestEventWithList(eventOffset: Long, eventHashValue: Long, eventType: String, createdDate: Date,
                             someObjects: List[SomeObject])  extends BaseEvent {

  def this(){
    this(0, 0, "", new Date(), List.empty)
  }

  override def copyy(eventOffset: Long, eventHashValue: Long): BaseEvent = {
    this.copy(eventOffset = eventOffset, eventHashValue = eventHashValue)
  }
}
