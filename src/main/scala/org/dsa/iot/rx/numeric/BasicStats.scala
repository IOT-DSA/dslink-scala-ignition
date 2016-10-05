package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Computes the statistics parameters of the source elements. Emits either the rolling stats or only
 * the final value.
 */
class BasicStats[T](implicit num: Numeric[T]) extends RxTransformer[T, Stats] {

  def running(): BasicStats[T] = this having (rolling <~ true)
  def single(): BasicStats[T] = this having (rolling <~ false)

  val rolling = Port[Boolean]("rolling")

  protected def compute = rolling.in flatMap { roll =>
    val values = source.in map num.toDouble
    if (roll)
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
   * Creates a new rolling BasicStats instance.
   */
  def apply[T](implicit num: Numeric[T]): BasicStats[T] = BasicStats(true)

  /**
   * Creates a new BasicStats instance.
   */
  def apply[T](rolling: Boolean)(implicit num: Numeric[T]): BasicStats[T] = {
    val block = new BasicStats
    block.rolling <~ rolling
    block
  }
}