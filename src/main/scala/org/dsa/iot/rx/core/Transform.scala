package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Transforms each item of the source sequence into a new one, using the functional operator.
 *
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/map.png" alt="" />
 */
class Transform[T, R] extends RxTransformer[T, R] {

  def operator(func: T => R): Transform[T, R] = this having (operator <~ func)

  val operator = Port[T => R]("operator")

  protected def compute = operator.in flatMap source.in.map
}

/**
 * Factory for [[Transform]] instances.
 */
object Transform {

  /**
   * Creates a new Transform instance.
   */
  def apply[T, R]: Transform[T, R] = new Transform[T, R]

  /**
   * Creates a new Transform instance that applies the supplied `func` to each source item.
   */
  def apply[T, R](func: T => R): Transform[T, R] = {
    val block = new Transform[T, R]
    block.operator <~ func
    block
  }
}