package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Computes the product of the source elements. Emits either the rolling product or only the final value.
 */
class Mul[T](implicit num: Numeric[T]) extends RxTransformer[T, T] {

  def running(): Mul[T] = this having (rolling <~ true)
  def single(): Mul[T] = this having (rolling <~ false)

  val rolling = Port[Boolean]("rolling")

  protected def compute = rolling.in flatMap {
    case true  => source.in.scan(num.times)
    case false => source.in.product
  }
}

/**
 * Factory for [[Mul]] instances.
 */
object Mul {

  /**
   * Creates a new rolling Mul instance.
   */
  def apply[T](implicit num: Numeric[T]): Mul[T] = Mul(true)

  /**
   * Creates a new Mul instance.
   */
  def apply[T](rolling: Boolean)(implicit num: Numeric[T]): Mul[T] = {
    val block = new Mul
    block.rolling <~ rolling
    block
  }
}