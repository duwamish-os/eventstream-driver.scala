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

  def fromPayload(payload: String): BaseEvent = {
    fromPayload(EventOffsetAndHashValue(0, 0), payload)
  }

  //FIXME make a way to have impl in trait itself so that there's no need to
  //have impl in each concrete impl
  def fromPayload(offset: EventOffsetAndHashValue, payload: String): BaseEvent

}

case class AbstractEvent(eventOffset: Long, eventHashValue: Long, eventType: String, createdDate: Date)
  extends BaseEvent {

  override def fromPayload(offset: EventOffsetAndHashValue, payload: String): BaseEvent = {
    AbstractEvent(0, 0, payload, new Date())
  }
}
