package org.dsa.iot.rx.core

import scala.concurrent.duration.{ Duration, DurationInt }
import org.dsa.iot.scala.Having

import org.dsa.iot.rx.RxTransformer

/**
 * Emits the results of sampling the items emitted by the source at a specified time interval.
 *
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/sample.png" alt="" />
 */
class Sample[T] extends RxTransformer[T, T] {

  def period(time: Duration): Sample[T] = this having (period <~ time)

  val period = Port[Duration]("period")

  protected def compute = period.in flatMap source.in.sample
}

/**
 * Factory for [[Sample]] instances.
 */
object Sample {

  /**
   * Creates a new Sample instance that samples data every second.
   */
  def apply[T]: Sample[T] = Sample(1 second)

  /**
   * Creates a new Sample instance with the specified sampling interval.
   */
  def apply[T](period: Duration): Sample[T] = {
    val block = new Sample[T]
    block.period <~ period
    block
  }
}