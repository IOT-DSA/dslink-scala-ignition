package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.RxTransformer

/**
 * Computes the minimum of the source elements. Emits either the rolling minimum or only the final value.
 */
class Min[T](rolling: Boolean)(implicit num: Numeric[T]) extends RxTransformer[T, T] {
  protected def compute = if (rolling) source.in.scan(num.min) else source.in.reduce(num.min)
}

/**
 * Factory for [[Min]] instances.
 */
object Min {

  /**
   * Creates a new Min instance.
   */
  def apply[T](rolling: Boolean)(implicit num: Numeric[T]): Min[T] = new Min(rolling)
}