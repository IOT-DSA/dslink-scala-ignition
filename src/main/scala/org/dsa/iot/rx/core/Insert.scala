package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Emits a specified item either before or after it emits items from the source.
 *
 * <img width="640" height="315" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/startWith.png" alt="" />
 */
class Insert[T] extends RxTransformer[T, T] {

  def before(): Insert[T] = this having (prepend <~ true)
  def after(): Insert[T] = this having (prepend <~ false)
  def item(value: T): Insert[T] = this having (item <~ value)

  val prepend = Port[Boolean]("prepend")
  val item = Port[T]("item")

  protected def compute = prepend.in combineLatest item.in flatMap {
    case (true, value)  => value +: source.in
    case (false, value) => source.in :+ value
  }
}

/**
 * Factory for [[Insert]] instances.
 */
object Insert {

  /**
   * Creates a new Insert instance.
   */
  def apply[T]: Insert[T] = new Insert[T]

  /**
   * Creates a new Insert instance with the specified item to prepend/append.
   */
  def apply[T](item: T, prepend: Boolean): Insert[T] = {
    val block = new Insert[T]
    block.prepend <~ prepend
    block.item <~ item
    block
  }
}