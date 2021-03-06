package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Applies a partial function to all elements emitted by the source.
 */
class Collect[T, R] extends RxTransformer[T, R] {

  def selector(func: PartialFunction[T, R]): Collect[T, R] = this having (selector <~ func)

  val selector = Port[PartialFunction[T, R]]("selector")

  protected def compute = selector.in flatMap source.in.collect
}

/**
 * Factory for [[Collect]] instances.
 */
object Collect {

  /**
   * Creates a new Collect instance.
   */
  def apply[T, R]: Collect[T, R] = new Collect[T, R]

  /**
   * Creates a new Collect instance that applies the supplied `func` to each source item.
   */
  def apply[T, R](func: PartialFunction[T, R]): Collect[T, R] = {
    val block = new Collect[T, R]
    block.selector <~ func
    block
  }
}