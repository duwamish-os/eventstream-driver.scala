package consumer.generic.two

import java.util.Date

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import event.BaseEvent
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by prayagupd
  * on 1/29/17.
  */

class GenericEventConsumerIntegrationSpecs extends FunSuite with BeforeAndAfterEach {

  val genericEventConsumer = new GenericEventConsumer[TestEvent1, TestEvent2](List("eventstream_with_two_event_types"))

  override protected def beforeEach(): Unit = EmbeddedKafka.start()

  override protected def afterEach(): Unit = EmbeddedKafka.stop()

  test("consumes a event1, given a consumer configured to consume two types of events") {

  }

}

@JsonIgnoreProperties(Array("eventOffset", "eventHashValue"))
case class TestEvent1(eventOffset: Long, eventHashValue: Long, eventType: String,
                             createdDate: Date, field1: String) extends BaseEvent {

  def this() {
    this(0, 0, "", new Date(), "")
  }

  override def toString: String = toJSON(this.copy())

  override def copyy(eventOffset: Long, eventHashValue: Long): BaseEvent = {
    this.copy(eventOffset = eventOffset, eventHashValue = eventHashValue)
  }
}

@JsonIgnoreProperties(Array("eventOffset", "eventHashValue"))
case class TestEvent2(eventOffset: Long, eventHashValue: Long, eventType: String,
                      createdDate: Date, field1: String) extends BaseEvent {

  def this() {
    this(0, 0, "", new Date(), "")
  }

  override def toString: String = toJSON(this.copy())

  override def copyy(eventOffset: Long, eventHashValue: Long): BaseEvent = {
    this.copy(eventOffset = eventOffset, eventHashValue = eventHashValue)
  }
}
