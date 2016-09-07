package org.dsa.iot.rx.core

import scala.concurrent.duration.Duration

import org.dsa.iot.rx.AbstractRxBlock

import rx.lang.scala.Observable

/**
 * Emits values 0, 1, 2 and so on at equal time intervals.
 */
class Interval extends AbstractRxBlock[Long] {
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
   * Creates a new Interval instance.
   */
  def apply(): Interval = new Interval
 
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