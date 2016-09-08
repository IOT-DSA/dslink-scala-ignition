package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.RxTransformer

/**
 * Computes the average of the source elements. Emits either the rolling average or only the final value.
 */
class Avg[T](rolling: Boolean)(implicit num: Numeric[T]) extends RxTransformer[T, Double] {

  type SumCount = (Double, Int)
  private val zero = (0.0, 0)
  private val accumulator = (sc: SumCount, next: Double) => (sc._1 + next, sc._2 + 1)

  protected def compute = {
    val values = source.in map num.toDouble

    val pairs = if (rolling) values.scan(zero)(accumulator) else values.foldLeft(zero)(accumulator)
    pairs map {
      case (sum, count) => if (count == 0) Double.NaN else sum / count
    }
  }
}

/**
 * Factory for [[Avg]] instances.
 */
object Avg {

  /**
   * Creates a new Avg instance.
   */
  def apply[T](rolling: Boolean)(implicit num: Numeric[T]): Avg[T] = new Avg(rolling)
}