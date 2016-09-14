package org.dsa.iot.rx.core

import org.dsa.iot.rx.AbstractRxBlock

import rx.lang.scala.Observable

/**
 * Emits no data and completes immediately.
 * 
 * <img width="640" height="190" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/empty.s.png" alt="" />
 */
class Empty extends AbstractRxBlock[Nothing] {
  protected def compute = Observable.empty
}

/**
 * Factory for [[Empty]] instances.
 */
object Empty {

  /**
   * Creates a new Empty instance.
   */
  def apply(): Empty = new Empty
}