package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Applies the selector function to the source sequence and then drops the repeated items.
 * If `global` parameter is ''false'' it deletes sequential repeated elements; if it is ''true'',
 * then it deletes '''all''' repeated elements.
 *
 * <img width="640" height="310" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/distinct.key.png" alt="" />
 * <img width="640" height="310" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/distinctUntilChanged.key.png" alt="" />
 */
class Distinct[T] extends RxTransformer[T, T] {

  def selector(func: T => _): Distinct[T] = this having (selector <~ func)
  def globally(): Distinct[T] = this having (global <~ true)
  def locally(): Distinct[T] = this having (global <~ false)

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
   * Creates a new Distinct instance with global identity selector.
   */
  def apply[T]: Distinct[T] = Distinct[T](true, identity[T] _)

  /**
   * Creates a new Distinct instance with the specified selector and global vs sequential flag.
   */
  def apply[T](global: Boolean, selector: T => _): Distinct[T] = {
    val block = new Distinct[T]
    block.selector <~ selector
    block.global <~ global
    block
  }
}