package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Counts the number of elements in the source sequence that satisfy the predicate. 
 * It can either produce a rolling count on each item, or just one final result when source 
 * sequence is complete.
 */
class Count[T](rolling: Boolean) extends RxTransformer[T, Int] {
  val predicate = Port[T => Boolean]("predicate")

  protected def compute = predicate.in flatMap { func =>
    if (rolling)
      source.in.filter(func).scan(0)((total, _) => total + 1)
    else
      source.in.count(func)
  }
}

/**
 * Factory for [[Count]] instances.
 */
object Count {

  /**
   * Creates a new Count instance which counts all elements - either as a running total
   * or as a final value only.
   */
  def apply[T](rolling: Boolean): Count[T] = apply(_ => true, rolling)

  /**
   * Creates a new Count instance for the specified predicate - either as a running total
   * or as a final value only.
   */
  def apply[T](predicate: T => Boolean, rolling: Boolean): Count[T] = {
    val block = new Count[T](rolling)
    block.predicate <~ predicate
    block
  }
}