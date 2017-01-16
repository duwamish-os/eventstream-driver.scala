package producer

import java.util.Date

import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

/**
  * Created by prayagupd
  * on 1/15/17.
  */

case class SomethingHappenedEvent(eventOffset: Long, hashValue: Long, created: Date) extends BaseEvent

class GenericEventPublisherSpecs extends FunSuite with MockFactory {

  val genericEventPublishertPublisher = new GenericEventPublisher
  genericEventPublishertPublisher.eventPublisher = mock[EventPublisher]

  test("delegates to the actual producer returned by factory") {
    val event = TestEvent(1, 2, new Date())

    (genericEventPublishertPublisher.eventPublisher.publish _) expects event
    genericEventPublishertPublisher.publish(event)
  }
}
