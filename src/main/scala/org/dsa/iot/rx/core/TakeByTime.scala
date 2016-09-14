package org.dsa.iot.rx.core

import scala.concurrent.duration.Duration

import org.dsa.iot.rx.RxTransformer

/**
 * Emits items from the source before a specified time runs out.
 * 
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/take.t.png" alt="" />
 */
class TakeByTime[T] extends RxTransformer[T, T] {
  val period = Port[Duration]("period")

  protected def compute = period.in flatMap source.in.take
}

/**
 * Factory for [[TakeByTime]] instances.
 */
object TakeByTime {

  /**
   * Creates a new TakeByTime instance.
   */
  def apply[T]: TakeByTime[T] = new TakeByTime[T]

  /**
   * Creates a new TakeByTime instance for the given time period.
   */
  def apply[T](period: Duration): TakeByTime[T] = {
    val block = new TakeByTime[T]
    block.period <~ period
    block
  }
}