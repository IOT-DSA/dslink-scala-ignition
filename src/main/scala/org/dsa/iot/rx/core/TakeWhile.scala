package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Emits items from the source as long as the predicate condition is true.
 * 
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/takeWhile.png" alt="" />
 */
class TakeWhile[T] extends RxTransformer[T, T] {
  
  def predicate(func: T => Boolean): TakeWhile[T] = this having (predicate <~ func)
  
  val predicate = Port[T => Boolean]("predicate")

  protected def compute = predicate.in flatMap source.in.takeWhile
}

/**
 * Factory for [[TakeWhile]] instances.
 */
object TakeWhile {

  /**
   * Creates a new TakeWhile instance.
   */
  def apply[T]: TakeWhile[T] = new TakeWhile[T]

  /**
   * Creates a new TakeWhile instance with the given predicate.
   */
  def apply[T](predicate: T => Boolean): TakeWhile[T] = {
    val block = new TakeWhile[T]
    block.predicate <~ predicate
    block
  }
}