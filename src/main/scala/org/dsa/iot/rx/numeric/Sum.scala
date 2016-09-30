package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Computes the sum of the source elements. Emits either the rolling sum or only the final value.
 */
class Sum[T](implicit num: Numeric[T]) extends RxTransformer[T, T] {

  def running(): Sum[T] = this having (rolling <~ true)
  def single(): Sum[T] = this having (rolling <~ false)

  val rolling = Port[Boolean]("rolling")

  protected def compute = rolling.in flatMap {
    case true  => source.in.scan(num.plus)
    case false => source.in.sum
  }
}

/**
 * Factory for [[Sum]] instances.
 */
object Sum {

  /**
   * Creates a new rolling Sum instance.
   */
  def apply[T](implicit num: Numeric[T]): Sum[T] = Sum(true)

  /**
   * Creates a new Sum instance.
   */
  def apply[T](rolling: Boolean)(implicit num: Numeric[T]): Sum[T] = {
    val block = new Sum
    block.rolling <~ rolling
    block
  }
}