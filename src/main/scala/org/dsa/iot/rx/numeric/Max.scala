package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Computes the maximum of the source elements. Emits either the rolling maximum or only the final value.
 */
class Max[T](implicit num: Numeric[T]) extends RxTransformer[T, T] {

  def running(): Max[T] = this having (rolling <~ true)
  def single(): Max[T] = this having (rolling <~ false)

  val rolling = Port[Boolean]("rolling")

  protected def compute = rolling.in flatMap {
    case true  => source.in.scan(num.max)
    case false => source.in.reduce(num.max)
  }
}

/**
 * Factory for [[Max]] instances.
 */
object Max {

  /**
   * Creates a new rolling Max instance.
   */
  def apply[T](implicit num: Numeric[T]): Max[T] = Max(true)

  /**
   * Creates a new Max instance.
   */
  def apply[T](rolling: Boolean)(implicit num: Numeric[T]): Max[T] = {
    val block = new Max
    block.rolling <~ rolling
    block
  }
}