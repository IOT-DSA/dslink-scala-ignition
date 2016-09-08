package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.RxTransformer

/**
 * Computes the sum of the source elements. Emits either the rolling sum or only the final value.
 */
class Sum[T](rolling: Boolean)(implicit num: Numeric[T]) extends RxTransformer[T, T] {
  protected def compute = if (rolling) source.in.scan(num.plus) else source.in.sum
}

/**
 * Factory for [[Sum]] instances.
 */
object Sum {

  /**
   * Creates a new Sum instance.
   */
  def apply[T](rolling: Boolean)(implicit num: Numeric[T]): Sum[T] = new Sum(rolling)
}