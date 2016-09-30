package org.dsa.iot.rx.core

import scala.concurrent.duration.{ Duration, DurationInt }

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Emits items from the source before a specified time runs out.
 *
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/take.t.png" alt="" />
 */
class TakeByTime[T] extends RxTransformer[T, T] {

  def period(time: Duration): TakeByTime[T] = this having (period <~ time)

  val period = Port[Duration]("period")

  protected def compute = period.in flatMap source.in.take
}

/**
 * Factory for [[TakeByTime]] instances.
 */
object TakeByTime {

  /**
   * Creates a new TakeByTime instance that takes elements within 1 second from
   * the beginning.
   */
  def apply[T]: TakeByTime[T] = TakeByTime(1 second)

  /**
   * Creates a new TakeByTime instance for the given time period.
   */
  def apply[T](period: Duration): TakeByTime[T] = {
    val block = new TakeByTime[T]
    block.period <~ period
    block
  }
}