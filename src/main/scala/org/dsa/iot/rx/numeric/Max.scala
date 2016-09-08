package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.RxTransformer

/**
 * Computes the maximum of the source elements. Emits either the rolling maximum or only the final value.
 */
class Max[T](rolling: Boolean)(implicit num: Numeric[T]) extends RxTransformer[T, T] {
  protected def compute = if (rolling) source.in.scan(num.max) else source.in.reduce(num.max)
}

/**
 * Factory for [[Max]] instances.
 */
object Max {

  /**
   * Creates a new Max instance.
   */
  def apply[T](rolling: Boolean)(implicit num: Numeric[T]): Max[T] = new Max(rolling)
}