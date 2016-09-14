package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.RxTransformer

/**
 * Computes the product of the source elements. Emits either the rolling product or only the final value.
 */
class Mul[T](rolling: Boolean)(implicit num: Numeric[T]) extends RxTransformer[T, T] {
  protected def compute = if (rolling) source.in.scan(num.times) else source.in.product
}

/**
 * Factory for [[Mul]] instances.
 */
object Mul {

  /**
   * Creates a new Mul instance.
   */
  def apply[T](rolling: Boolean)(implicit num: Numeric[T]): Mul[T] = new Mul(rolling)
}