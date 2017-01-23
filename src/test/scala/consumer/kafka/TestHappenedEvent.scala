package consumer.kafka

import java.io.ByteArrayOutputStream
import java.util.Date

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import offset.EventOffsetAndHashValue
import producer.BaseEvent

/**
  * Created by prayagupd
  * on 1/19/17.
  */

@JsonIgnoreProperties(Array("eventOffset", "hashValue"))
case class TestHappenedEvent(eventOffset: Long, hashValue: Long, eventType: String,
                             createdDate: Date, testField: String) extends BaseEvent {

  def this() {
    this(0, 0, "", new Date(), "")
  }

  override def fromPayload(offset: EventOffsetAndHashValue, payload: String): BaseEvent = {

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    mapper.readValue(payload, classOf[TestHappenedEvent])
      .copy(eventOffset = offset.offset, hashValue = offset.checksum)
  }

  override def toJSON(): String = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)

    val data = this.copy()
    val stream = new ByteArrayOutputStream()
    objectMapper.writeValue(stream, data)
    stream.toString
  }
}
