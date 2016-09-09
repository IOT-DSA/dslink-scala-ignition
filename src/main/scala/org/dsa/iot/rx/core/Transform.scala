package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Transforms each item of the source sequence into a new one, using the functional operator.
 */
class Transform[T, R] extends RxTransformer[T, R] {
  val operator = Port[T => R]("operator")

  protected def compute = operator.in flatMap source.in.map
}

/**
 * Factory for [[Transform]] instances.
 */
object Transform {

  /**
   * Creates a new Transform instance that does not apply any transformation (identity transformation).
   */
  def apply[T]: Transform[T, T] = create(identity[T])

  /**
   * Creates a new Transform instance that applies the supplied `func` to each source item.
   */
  def apply[T, R](func: T => R): Transform[T, R] = create(func)

  private def create[T, R](func: T => R) = {
    val block = new Transform[T, R]
    block.operator <~ func
    block
  }
}