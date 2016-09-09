package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Emits a specified item either before or after it emits items from the source.
 */
class Insert[T](prepend: Boolean) extends RxTransformer[T, T] {
  val item = Port[T]("item")

  protected def compute = prepend match {
    case true  => item.in flatMap (_ +: source.in)
    case false => item.in flatMap (source.in :+ _)
  }
}

/**
 * Factory for [[Insert]] instances.
 */
object Insert {

  /**
   * Creates a new Insert instance.
   */
  def apply[T](prepend: Boolean): Insert[T] = new Insert[T](prepend)

  /**
   * Creates a new Insert instance with the specified item to prepend/append.
   */
  def apply[T](item: T, prepend: Boolean): Insert[T] = {
    val block = new Insert[T](prepend)
    block.item <~ item
    block
  }
}