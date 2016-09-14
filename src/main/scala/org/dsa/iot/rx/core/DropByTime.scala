package org.dsa.iot.rx.core

import scala.concurrent.duration.Duration

import org.dsa.iot.rx.RxTransformer

/**
 * Drops items from either a beginning or end of the source sequence, for the duration of the
 * specified time window.
 * 
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/skip.t.png" alt="" />
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/skipLast.t.png" alt="" />
 */
class DropByTime[T](right: Boolean) extends RxTransformer[T, T] {
  val period = Port[Duration]("period")

  protected def compute = right match {
    case true  => period.in flatMap source.in.dropRight
    case false => period.in flatMap source.in.drop
  }
}

/**
 * Factory for [[DropByTime]] instances.
 */
object DropByTime {

  /**
   * Creates a new DropByTime instance. If `right` is true, drops items from the end of the sequence,
   * otherwise drops items from the beginning.
   */
  def apply[T](right: Boolean): DropByTime[T] = new DropByTime[T](right)

  /**
   * Creates a new DropByTime instance for the given time period. If `right` is true, drops items
   * from the end of the sequence, otherwise drops items from the beginning.
   */
  def apply[T](period: Duration, right: Boolean): DropByTime[T] = {
    val block = new DropByTime[T](right)
    block.period <~ period
    block
  }
}