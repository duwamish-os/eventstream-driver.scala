package event

import java.io.ByteArrayOutputStream
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
  * Created by prayagupd
  * on 1/23/17.
  */

trait BaseEvent {
  def eventOffset: Long

  def eventHashValue: Long

  def eventType: String

  def createdDate: Date

  def toJSON[E <: BaseEvent](event: E): String = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)

    val stream = new ByteArrayOutputStream()
    objectMapper.writeValue(stream, event)
    stream.toString
  }

  def fromPayload[E <: BaseEvent](payload: String, eventType: Class[E]): BaseEvent = {
    fromPayload(EventOffsetAndHashValue(0, 0), payload, eventType)
  }

  def fromPayload[E <: BaseEvent](offset: EventOffsetAndHashValue, payload: String, eventType: Class[E]): BaseEvent = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    mapper.readValue(payload, eventType)
      .copyy(eventOffset = offset.offset, eventHashValue = offset.checksum)
  }

  def copyy(eventOffset: Long, eventHashValue: Long) : BaseEvent
}
