package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Only emits those items from the source sequence for which a given predicate holds.
 * 
 * <img width="640" height="310" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/filter.png" alt="" />
 */
class Filter[T] extends RxTransformer[T, T] {
  
  def predicate(func: T => Boolean): Filter[T] = this having (predicate <~ func)
  
  val predicate = Port[T => Boolean]("predicate")

  protected def compute = predicate.in flatMap source.in.filter
}

/**
 * Factory for [[Filter]] instances.
 */
object Filter {

  /**
   * Creates a new Filter instance.
   */
  def apply[T]: Filter[T] = new Filter[T]

  /**
   * Creates a new Filter instance for the specified predicate.
   */
  def apply[T](predicate: T => Boolean): Filter[T] = {
    val block = new Filter[T]
    block.predicate <~ predicate
    block
  }
}