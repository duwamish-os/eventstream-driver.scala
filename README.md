streaming driver
------------------

usage
-----

```
streaming {
  driver = Kafka
}
```

Producer
--------

```scala
 val eventPublisher = new GenericEventPublisher
 val event = TestEvent(0, 0, new Date())
 val persistedEvent1 = eventPublisher.publish(TestEvent(0, 0, new Date()))
 assert(persistedEvent1.eventOffset == 0)
```


Consumer
--------

```scala
class TestEventHandler extends EventHandler[TestHappenedEvent] {

  override def onEvent(event: TestHappenedEvent): Unit = {
    println(s"event = ${event.testField}")
  }
}

```

```scala
  val consumer = new AbstractKafkaEventConsumer[TestHappenedEvent] {
    addConfiguration(new Properties() {{
        put("group.id", "consumers_testEventsGroup")
        put("client.id", "testKafkaEventConsumer")
        put("auto.offset.reset", "earliest")
      }})
      .subscribeEvents(List(classOf[TestHappenedEvent].getSimpleName))
      .setEventHandler(new TestEventHandler)
      .setEventType(classOf[TestHappenedEvent])
  }
  
  val events = consumer.consumeAll()
```