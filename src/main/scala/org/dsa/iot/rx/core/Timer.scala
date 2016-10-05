package org.dsa.iot.rx.core

import scala.concurrent.duration.{ Duration, DurationInt }

import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.Having

import rx.lang.scala.Observable

/**
 * Emits 0L after the specified delay and completes.
 *
 * <img width="640" height="200" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/timer.png" alt="" />
 */
class Timer extends AbstractRxBlock[Long] {

  def delay(time: Duration): Timer = this having (delay <~ time)

  val delay = Port[Duration]("delay")

  protected def compute = delay.in flatMap Observable.timer
}

/**
 * Factory for [[Timer]] instances.
 */
object Timer {

  /**
   * Creates a new Timer instance with 1 second delay.
   */
  def apply(): Timer = Timer(1 second)

  /**
   * Creates a new Timer instance with the specified delay.
   */
  def apply(delay: Duration): Timer = {
    val block = new Timer
    block.delay <~ delay
    block
  }
}