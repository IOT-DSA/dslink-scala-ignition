package org.dsa.iot.rx.core

import scala.concurrent.duration.{ Duration, DurationInt }

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Emits the items emitted by the source shifted forward in time by a specified delay.
 *
 * <img width="640" height="310" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/delay.png" alt="" />
 */
class Delay[T] extends RxTransformer[T, T] {

  def period(time: Duration): Delay[T] = this having (period <~ time)

  val period = Port[Duration]("period")

  protected def compute = period.in flatMap source.in.delay
}

/**
 * Factory for [[Delay]] instances.
 */
object Delay {

  /**
   * Creates a new Delay instance with the 1 second delay.
   */
  def apply[T]: Delay[T] = Delay[T](1 second)

  /**
   * Creates a new Delay instance for the specified delay time.
   */
  def apply[T](period: Duration): Delay[T] = {
    val block = new Delay[T]
    block.period <~ period
    block
  }
}