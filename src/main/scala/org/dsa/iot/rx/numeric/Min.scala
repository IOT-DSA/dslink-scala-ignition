package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Computes the minimum of the source elements. Emits either the rolling minimum or only the final value.
 */
class Min[T](implicit num: Numeric[T]) extends RxTransformer[T, T] {

  def running(): Min[T] = this having (rolling <~ true)
  def single(): Min[T] = this having (rolling <~ false)

  val rolling = Port[Boolean]("rolling")

  protected def compute = rolling.in flatMap {
    case true  => source.in.scan(num.min)
    case false => source.in.reduce(num.min)
  }
}

/**
 * Factory for [[Min]] instances.
 */
object Min {

  /**
   * Creates a new rolling Min instance.
   */
  def apply[T](implicit num: Numeric[T]): Min[T] = Min(true)

  /**
   * Creates a new Min instance.
   */
  def apply[T](rolling: Boolean)(implicit num: Numeric[T]): Min[T] = {
    val block = new Min
    block.rolling <~ rolling
    block
  }
}