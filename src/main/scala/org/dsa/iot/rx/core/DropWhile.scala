package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Drops items from the source as long as the predicate condition is true.
 *
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/skipWhile.png" alt="" />
 */
class DropWhile[T] extends RxTransformer[T, T] {

  def predicate(func: T => Boolean): DropWhile[T] = this having (predicate <~ func)

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