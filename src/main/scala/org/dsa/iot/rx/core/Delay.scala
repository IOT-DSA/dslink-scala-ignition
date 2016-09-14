package org.dsa.iot.rx.core

import scala.concurrent.duration.Duration

import org.dsa.iot.rx.RxTransformer

/**
 * Emits the items emitted by the source shifted forward in time by a specified delay.
 * 
 * <img width="640" height="310" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/delay.png" alt="" />
 */
class Delay[T] extends RxTransformer[T, T] {
  val period = Port[Duration]("period")

  protected def compute = period.in flatMap source.in.delay
}

/**
 * Factory for [[Delay]] instances.
 */
object Delay {
  
  /**
   * Creates a new Delay instance.
   */
  def apply[T]: Delay[T] = new Delay[T]
  
  /**
   * Creates a new Delay instance for the specified delay time.
   */
  def apply[T](period: Duration): Delay[T] = {
    val block = new Delay[T]
    block.period <~ period
    block
  }
}