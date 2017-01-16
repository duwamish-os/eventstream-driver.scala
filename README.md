streaming driver
------------------


```
streaming {
  driver = Kafka
}
```

usage
-----

```scala
    val eventPublisher = new GenericEventPublisher
    val event = TestEvent(0, 0, new Date())
    val persistedEvent1 = eventPublisher.publish(TestEvent(0, 0, new Date()))
    assert(persistedEvent1.eventOffset == 0)
```