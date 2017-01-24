package consumer

import event.BaseEvent

/**
  * Created by prayagupd
  * on 1/23/17.
  */

trait EventHandler[E <: BaseEvent] {
  def onEvent(event: E)
}
