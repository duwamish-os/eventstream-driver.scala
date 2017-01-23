package producer

import java.util.Date

import consumer.kafka.TestHappenedEvent
import offset.EventOffsetAndHashValue
import org.scalatest.FunSuite
import producer.kafka.{SomeObject, TestEventWithList}
import spray.json._

/**
  * Created by prayagupd
  * on 1/20/17.
  */

class BaseEventSpecs extends FunSuite {

  val abstractEvent = TestHappenedEvent(0, 1, "TestHappenedEvent", new Date(2017, 10, 28), "item is sold")

  case class Test(name: String)

  test("converts itself to JSON") {
    assert(abstractEvent.toJSON().parseJson ==
      """
        {
          "eventType":"TestHappenedEvent",
          "createdDate":61470000000000,
          "field1":"item is sold"
        }
      """.stripMargin.parseJson)
  }

  test("toString also converts itself to JSON") {
    assert(abstractEvent.toString().parseJson ==
      """
        {
          "eventType":"TestHappenedEvent",
          "createdDate":61470000000000,
          "field1":"item is sold"
        }
      """.stripMargin.parseJson)
  }

  test("converts json payload to object") {
    val payload =
      """{
          "eventType":"TestHappenedEvent",
          "createdDate":61470000000000,
          "field1":"item is sold"
        }""".stripMargin

    val actual = new TestHappenedEvent().fromPayload(offset = EventOffsetAndHashValue(0, 10001), payload = payload)
      .asInstanceOf[TestHappenedEvent]

    assert(actual.eventHashValue == 10001)
    assert(actual.eventType == classOf[TestHappenedEvent].getSimpleName)
    assert(actual.createdDate == new Date(2017, 10, 28))
    assert(actual.field1 == "item is sold")
  }

  test("converts json payload with empty list elements to object") {
    val payload =
      """{
          "eventType":"TestEventWithList",
          "createdDate":61470000000000,
          "someObjects": []
        }""".stripMargin

    val actual = new TestEventWithList().fromPayload(payload = payload)
      .asInstanceOf[TestEventWithList]

    assert(actual.eventHashValue == 0)
    assert(actual.eventType == classOf[TestEventWithList].getSimpleName)
    assert(actual.createdDate == new Date(2017, 10, 28))
    assert(actual.someObjects == List.empty)
  }

  test("converts json payload with list elements to object") {
    val payload =
      """{
          "eventType":"TestEventWithList",
          "createdDate":61470000000000,
          "someObjects": [ {
                  "field1" : "value1"
          } ]
        }""".stripMargin

    val actual = new TestEventWithList().fromPayload(payload = payload)
      .asInstanceOf[TestEventWithList]

    assert(actual.eventHashValue == 0)
    assert(actual.eventType == classOf[TestEventWithList].getSimpleName)
    assert(actual.createdDate == new Date(2017, 10, 28))
    assert(actual.someObjects == List(SomeObject(field1 = "value1", field2 = null)))
  }

  test("converts json payload with multiple elements to object") {
    val payload =
      """{
          "eventType":"TestEventWithList",
          "createdDate":61470000000000,
          "someObjects": [ {
              "field1" : "value11"
          },
          {
              "field1" : "value21",
              "field2" : "value22"
          }]
        }""".stripMargin

    val actual = new TestEventWithList().fromPayload(payload = payload)
      .asInstanceOf[TestEventWithList]

    assert(actual.eventHashValue == 0)
    assert(actual.eventType == classOf[TestEventWithList].getSimpleName)
    assert(actual.createdDate == new Date(2017, 10, 28))
    assert(actual.someObjects == List(SomeObject(field1 = "value11", field2 = null),
      SomeObject(field1 = "value21", field2 = "value22")))
  }
}
