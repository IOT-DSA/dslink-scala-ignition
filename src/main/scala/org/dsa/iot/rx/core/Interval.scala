package org.dsa.iot.rx.core

import scala.concurrent.duration.{ Duration, DurationInt }

import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.Having

import rx.lang.scala.Observable

/**
 * Emits values 0, 1, 2 and so on at equal time intervals.
 *
 * <img width="640" height="200" src="https://raw.github.com/wiki/ReactiveX/RxJava/images/rx-operators/timer.p.png" alt="" />
 */
class Interval extends AbstractRxBlock[Long] {

  def initial(time: Duration): Interval = this having (initial <~ time)
  def period(time: Duration): Interval = this having (period <~ time)

  val initial = Port[Duration]("initial")
  val period = Port[Duration]("period")

  protected def compute = (initial.in combineLatest period.in) flatMap {
    case (i, p) => Observable.interval(i, p)
  }
}

/**
 * Factory for [[Interval]] instances.
 */
object Interval {

  /**
   * Creates a new Interval instance with 1 second interval and no initial delay.
   */
  def apply(): Interval = Interval(1 second, Duration.Zero)

  /**
   * Creates a new Interval instance with the given period and initial delay.
   */
  def apply(period: Duration, initial: Duration = Duration.Zero): Interval = {
    val block = new Interval
    block.initial <~ initial
    block.period <~ period
    block
  }
}