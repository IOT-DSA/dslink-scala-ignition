package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.RxTransformer

/**
 * Computes the statistics parameters of the source elements. Emits either the rolling stats or only 
 * the final value.
 */
class BasicStats[T](rolling: Boolean)(implicit num: Numeric[T]) extends RxTransformer[T, Stats] {
  
  protected def compute = {
    val values = source.in map num.toDouble
    if (rolling)
      values.scan(Stats.empty)(_ merge _)
    else
      values.foldLeft(Stats.empty)(_ merge _)
  }
}

/**
 * Factory for [[BasicStats]] instances.
 */
object BasicStats {

  /**
   * Creates a new BasicStats instance.
   */
  def apply[T](rolling: Boolean)(implicit num: Numeric[T]): BasicStats[T] = new BasicStats(rolling)
}