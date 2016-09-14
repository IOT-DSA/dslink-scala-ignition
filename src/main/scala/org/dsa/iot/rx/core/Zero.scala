package org.dsa.iot.rx.core

import org.dsa.iot.rx.AbstractRxBlock

import rx.lang.scala.Observable

/**
 * Emits a single "zero" item for the specified numeric type.
 */
class Zero[T](implicit num: Numeric[T]) extends AbstractRxBlock[T] {
  protected def compute = Observable.just(num.zero)
}

/**
 * Factory for [[Zero]] instances.
 */
object Zero {
  /**
   * Creates a new Zero instance for the specified numeric type.
   */
  def apply[T](implicit num: Numeric[T]): Zero[T] = new Zero[T]
}