package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Tests whether a predicate holds for some of the elements of the source.
 */
class Exists[T] extends RxTransformer[T, Boolean] {
  val predicate = Port[T => Boolean]("predicate")

  protected def compute = predicate.in flatMap source.in.exists
}

/**
 * Factory for [[Exists]] instances.
 */
object Exists {
  
  /**
   * Creates a new Exists instance.
   */
  def apply[T]: Exists[T] = new Exists[T]

  /**
   * Creates a new Exists instance for a given predicate.
   */
  def apply[T](predicate: T => Boolean): Exists[T] = {
    val block = new Exists[T]
    block.predicate <~ predicate
    block
  }
}