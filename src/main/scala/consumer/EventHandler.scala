package consumer

import event.BaseEvent

/**
  * Created by prayagupd
  * on 1/23/17.
  */

trait EventHandler[E <: BaseEvent] {
  def onEvent(event: E)
}

trait Handleable[E <: BaseEvent] {
  def consume(event: E): Unit
}

abstract class TwoEventsHandler[A, B] {
  def onEventA(event: A): Unit
  def onEventB(event: B): Unit
}
