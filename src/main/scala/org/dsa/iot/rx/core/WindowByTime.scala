package org.dsa.iot.rx.core

import scala.concurrent.duration.{ Duration, DurationInt }

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Creates an Observable which produces buffers of collected values.
 */
class WindowByTime[T] extends RxTransformer[T, Seq[T]] {

  def span(time: Duration): WindowByTime[T] = this having (span <~ time)
  def shift(time: Duration): WindowByTime[T] = this having (shift <~ time)

  val span = Port[Duration]("span")
  val shift = Port[Duration]("shift")

  protected def compute = (span.in combineLatest shift.in) flatMap {
    case (sp, sh) if sp == sh => source.in.tumblingBuffer(sp)
    case (sp, sh)             => source.in.slidingBuffer(sp, sh)
  }
}

/**
 * Factory for [[WindowByTime]] instances.
 */
object WindowByTime {

  /**
   * Creates a new WindowByTime instance of non-overlapping buffers of 10 seconds.
   */
  def apply[T]: WindowByTime[T] = WindowByTime(10 seconds)

  /**
   * Creates a new WindowByTime instance for non-overlapping buffers of `span` duration.
   */
  def apply[T](span: Duration): WindowByTime[T] = WindowByTime(span, span)

  /**
   * Creates a new WindowByTime instance for buffers of `span` duration created every `shift`.
   */
  def apply[T](span: Duration, shift: Duration): WindowByTime[T] = {
    val block = new WindowByTime[T]
    block.span <~ span
    block.shift <~ shift
    block
  }
}