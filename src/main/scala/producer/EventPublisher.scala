package producer

import java.io.ByteArrayOutputStream
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import offset.EventOffsetAndHashValue

/**
  * Created by prayagupd
  * on 1/13/17.
  */

trait BaseEvent {
  def eventOffset: Long

  def hashValue: Long

  def eventType: String

  def createdDate: Date

  def toJSON(): String

  def fromPayload(payload: String): BaseEvent = {
    fromPayload(EventOffsetAndHashValue(0, 0), payload)
  }

  def fromPayload(offset: EventOffsetAndHashValue, payload: String): BaseEvent

  override def toString: String = toJSON()
}

case class AbstractEvent(eventOffset: Long, hashValue: Long, eventType: String, createdDate: Date) extends BaseEvent {

  override def fromPayload(offset: EventOffsetAndHashValue, payload: String): BaseEvent = {
    AbstractEvent(0, 0, payload, new Date())
  }

  override def toJSON(): String = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)

    val stream = new ByteArrayOutputStream()
    objectMapper.writeValue(stream, this)
    stream.toString
  }
}

trait EventPublisher {
  def publish(event: BaseEvent): BaseEvent
}
