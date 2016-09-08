package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Applies the selector function to the source sequence and then drops the repeated items.
 * If `global` parameter is ''false'' it deletes sequential repeated elements; if it is ''true'',
 * then it deletes '''all''' repeated elements.
 */
class Distinct[T] extends RxTransformer[T, T] {
  val selector = Port[T => _]("selector")
  val global = Port[Boolean]("global")

  protected def combineAttributes = selector.in combineLatest global.in

  protected def inputs = source.in

  protected def compute = (selector.in combineLatest global.in) flatMap {
    case (func, true)  => source.in distinct func
    case (func, false) => source.in distinctUntilChanged func
  }
}

/**
 * Factory for [[Distinct]] instances.
 */
object Distinct {

  /**
   * Creates a new Distinct instance.
   */
  def apply[T]: Distinct[T] = new Distinct[T]

  /**
   * Creates a new Distinct instance with the specified selector and global vs sequential flag. 
   */
  def apply[T](global: Boolean, selector: T => _ = identity[T] _): Distinct[T] = {
    val block = new Distinct[T]
    block.selector <~ selector
    block.global <~ global
    block
  }
}