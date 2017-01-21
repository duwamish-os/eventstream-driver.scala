package producer

import java.util.Date

import consumer.kafka.TestHappenedEvent
import org.scalatest.FunSuite
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
          "testField":"item is sold"
        }
      """.stripMargin.parseJson)
  }

  test("toString also converts itself to JSON") {
    assert(abstractEvent.toString().parseJson ==
      """
        {
          "eventType":"TestHappenedEvent",
          "createdDate":61470000000000,
          "testField":"item is sold"
        }
      """.stripMargin.parseJson)
  }
}
