package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Emits the item found at a specified index in a sequence of emissions from a source,
 * or a default item if that index is out of range.
 */
class ElementAt[T] extends RxTransformer[T, T] {
  val index = Port[Int]("index")
  val default = Port[Option[T]]("default")

  protected def compute = (index.in combineLatest default.in) flatMap {
    case (index, Some(x)) => source.in.elementAtOrDefault(index, x)
    case (index, None)    => source.in.elementAt(index)
  }
}

/**
 * Factory for [[ElementAt]] instances.
 */
object ElementAt {

  /**
   * Creates a new ElementAt instance.
   */
  def apply[T]: ElementAt[T] = new ElementAt[T]

  /**
   * Creates a new ElementAt instance for the specified index and no default value.
   */
  def apply[T](index: Int): ElementAt[T] = create(index, None)

  /**
   * Creates a new ElementAt instance for the specified index and default value.
   */
  def apply[T](index: Int, default: T): ElementAt[T] = create(index, Some(default))

  private def create[T](index: Int, default: Option[T]) = {
    val block = new ElementAt[T]
    block.index <~ index
    block.default <~ default
    block
  }
}