package consumer.kafka

import java.io.ByteArrayOutputStream
import java.util.Date

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import event.{BaseEvent, EventOffsetAndHashValue}

/**
  * Created by prayagupd
  * on 1/19/17.
  */

@JsonIgnoreProperties(Array("eventOffset", "eventHashValue"))
case class TestHappenedEvent(eventOffset: Long, eventHashValue: Long, eventType: String,
                             createdDate: Date, field1: String) extends BaseEvent {

  def this() {
    this(0, 0, "", new Date(), "")
  }

  override def fromPayload(offset: EventOffsetAndHashValue, payload: String): BaseEvent = {

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    mapper.readValue(payload, classOf[TestHappenedEvent])
      .copy(eventOffset = offset.offset, eventHashValue = offset.checksum)
  }

  override def toString: String = toJSON(this.copy())
}
