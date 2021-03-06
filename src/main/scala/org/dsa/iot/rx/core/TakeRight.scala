package org.dsa.iot.rx.core

import scala.concurrent.duration.{ Duration, DurationInt }

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Emits items from the source that were emitted in a specified window of time before the source
 * completed, or the last `count` items of the source.
 *
 * <img width="640" height="310" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/takeLast.tn.png" alt="" />
 */
class TakeRight[T] extends RxTransformer[T, T] {

  def period(time: Duration): TakeRight[T] = this having (period <~ Some(time))
  def unlimitedTime(): TakeRight[T] = this having (period <~ None)
  def count(n: Int): TakeRight[T] = this having (count <~ Some(n))
  def unlimitedCount(): TakeRight[T] = this having (count <~ None)

  val period = Port[Option[Duration]]("period")
  val count = Port[Option[Int]]("count")

  protected def compute = period.in combineLatest count.in flatMap {
    case (Some(time), Some(num)) => source.in takeRight (num, time)
    case (Some(time), None)      => source.in takeRight time
    case (None, Some(num))       => source.in takeRight num
    case _                       => throw new IllegalArgumentException("Neither period nor count set")
  }
}

/**
 * Factory for [[TakeRight]] instances.
 */
object TakeRight {

  /**
   * Creates a new TakeRight instance for one second and count of one.
   */
  def apply[T]: TakeRight[T] = TakeRight(1 second, 1)

  /**
   * Creates a new TakeRight instance for the specified time period.
   */
  def apply[T](period: Duration): TakeRight[T] = create(Some(period), None)

  /**
   * Creates a new TakeRight instance for the specified count.
   */
  def apply[T](count: Int): TakeRight[T] = create(None, Some(count))

  /**
   * Creates a new TakeRight instance for the specified time period and the maximum number of items to emit.
   */
  def apply[T](period: Duration, count: Int): TakeRight[T] = create(Some(period), Some(count))

  /**
   * Creates a new TakeRight instance with optional `period` and `count` (at least one of them needs
   * to be set).
   */
  private def create[T](period: Option[Duration], count: Option[Int]) = {
    val block = new TakeRight[T]
    block.period <~ period
    block.count <~ count
    block
  }
}