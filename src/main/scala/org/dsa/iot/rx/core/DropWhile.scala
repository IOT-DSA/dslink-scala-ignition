package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Drops items from the source as long as the predicate condition is true.
 */
class DropWhile[T] extends RxTransformer[T, T] {
  val predicate = Port[T => Boolean]("predicate")

  protected def compute = predicate.in flatMap source.in.dropWhile
}

/**
 * Factory for [[DropWhile]] instances.
 */
object DropWhile {

  /**
   * Creates a new DropWhile instance.
   */
  def apply[T]: DropWhile[T] = new DropWhile[T]

  /**
   * Creates a new DropWhile instance with the given predicate.
   */
  def apply[T](predicate: T => Boolean): DropWhile[T] = {
    val block = new DropWhile[T]
    block.predicate <~ predicate
    block
  }
}