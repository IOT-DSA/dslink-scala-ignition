package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Only emits those items from the source sequence for which a given predicate holds.
 */
class Filter[T] extends RxTransformer[T, T] {
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