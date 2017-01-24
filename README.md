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

generic producer config

```
bootstrap.servers=localhost:9092
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

```scala
 val eventPublisher = new GenericEventPublisher
 val event = TestEvent(0, 0, new Date())
 val persistedEvent1 = eventPublisher.publish(TestEvent(0, 0, new Date()))
 assert(persistedEvent1.eventOffset == 0)
```


Consumer
--------

generic consumer config

```
bootstrap.servers=localhost:9092
enable.auto.commit=true
auto.commit.interval.ms=1000
session.timeout.ms=30000
auto.offset.reset=latest
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

```scala
class TestEventHandler extends EventHandler[TestHappenedEvent] {

  override def onEvent(event: TestHappenedEvent): Unit = {
    println(s"event = ${event}")
  }
}

```

```scala
  val consumer = new GenericEventConsumer[TestHappenedEvent] {
    addConfiguration(new Properties() {{
        put("group.id", "consumers_testEventsGroup")
        put("client.id", "testGenericEventConsumer")
        put("auto.offset.reset", "earliest") //override default config
      }})
      .subscribeEvents(List(classOf[TestHappenedEvent]))
      .setEventHandler(new TestEventHandler)
   }
  
  val events = consumer.consumeAll()
```

test
----

```
sbt clean test
```