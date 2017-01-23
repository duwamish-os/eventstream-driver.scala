package producer

import java.io.ByteArrayOutputStream
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import offset.EventOffsetAndHashValue
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

/**
  * Created by prayagupd
  * on 1/15/17.
  */

case class SomethingHappenedEvent(eventOffset: Long, hashValue: Long, eventType: String, createdDate: Date) extends BaseEvent {
  override def fromPayload(offset: EventOffsetAndHashValue, payload: String): BaseEvent = null

  override def toJSON(): String = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)

    val stream = new ByteArrayOutputStream()
    objectMapper.writeValue(stream, this)
    stream.toString
  }
}

class GenericEventPublisherSpecs extends FunSuite with MockFactory {

  val genericEventPublishertPublisher = new GenericEventPublisher
  genericEventPublishertPublisher.eventPublisher = mock[EventPublisher]

  test("delegates to the actual producer returned by factory") {
    val event = SomethingHappenedEvent(1, 2, classOf[SomethingHappenedEvent].getSimpleName, new Date())

    (genericEventPublishertPublisher.eventPublisher.publish _) expects event
    genericEventPublishertPublisher.publish(event)
  }
}
