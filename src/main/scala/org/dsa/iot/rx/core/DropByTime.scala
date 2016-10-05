package org.dsa.iot.rx.core

import scala.concurrent.duration.{ Duration, DurationInt }

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Drops items from either a beginning or end of the source sequence, for the duration of the
 * specified time window.
 *
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/skip.t.png" alt="" />
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/skipLast.t.png" alt="" />
 */
class DropByTime[T] extends RxTransformer[T, T] {
  
  def fromRight(): DropByTime[T] = this having (right <~ true)
  def fromLeft(): DropByTime[T] = this having (right <~ false)
  def period(time: Duration): DropByTime[T] = this having (period <~ time)

  val right = Port[Boolean]("right")
  val period = Port[Duration]("period")

  protected def compute = right.in combineLatest period.in flatMap {
    case (true, time)  => source.in.dropRight(time)
    case (false, time) => source.in.drop(time)
  }
}

/**
 * Factory for [[DropByTime]] instances.
 */
object DropByTime {

  /**
   * Creates a new DropByTime instance for dropping elemense within 1 second from the
   * beginning.
   */
  def apply[T]: DropByTime[T] = DropByTime(1 second, false)

  /**
   * Creates a new DropByTime instance for the given time period. If `right` is true, drops items
   * from the end of the sequence, otherwise drops items from the beginning.
   */
  def apply[T](period: Duration, right: Boolean): DropByTime[T] = {
    val block = new DropByTime[T]
    block.right <~ right
    block.period <~ period
    block
  }
}