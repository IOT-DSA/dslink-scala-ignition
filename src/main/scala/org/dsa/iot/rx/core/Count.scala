package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Counts the number of elements in the source sequence that satisfy the predicate.
 * It can either produce a rolling count on each item, or just one final result when source
 * sequence is complete.
 */
class Count[T] extends RxTransformer[T, Int] {

  def running(): Count[T] = this having (rolling <~ true)
  def single(): Count[T] = this having (rolling <~ false)
  def predicate(func: T => Boolean): Count[T] = this having (predicate <~ func)

  val rolling = Port[Boolean]("rolling")
  val predicate = Port[T => Boolean]("predicate")

  protected def compute = rolling.in combineLatest predicate.in flatMap {
    case (true, func)  => source.in.filter(func).scan(0)((total, _) => total + 1)
    case (false, func) => source.in.count(func)
  }
}

/**
 * Factory for [[Count]] instances.
 */
object Count {

  /**
   * Creates a new Count instance which counts all elements as a running total.
   */
  def apply[T]: Count[T] = apply(_ => true, true)

  /**
   * Creates a new Count instance for the specified predicate - either as a running total
   * or as a final value only.
   */
  def apply[T](predicate: T => Boolean, rolling: Boolean): Count[T] = {
    val block = new Count[T]
    block.rolling <~ rolling
    block.predicate <~ predicate
    block
  }
}