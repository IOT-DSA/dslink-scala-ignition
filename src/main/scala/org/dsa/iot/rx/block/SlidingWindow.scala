package org.dsa.iot.rx.block

import scala.concurrent.duration.DurationLong

import org.dsa.iot.{ listToValue, valueToAny, valueToInt, valueToLong }
import org.dsa.iot.dslink.node.value.Value

case class SlidingWindowBySize() extends DSARxBlock {
  val count = Port[Value]
  val skip = Port[Value]
  val input = Port[Value]

  // use Observable.combineLatest(Seq) ???
  protected def combineAttributes = count.in combineLatest skip.in map {
    case (c, p) => Seq(c, p)
  }

  protected def combineInputs = Seq(input.in)

  protected def evaluator(attrs: Seq[Value]) = _.head.slidingBuffer(attrs(0), attrs(1)) map (_.map(valueToAny).toList)
}

case class SlidingWindowByTime() extends DSARxBlock {
  val span = Port[Value]
  val shift = Port[Value]
  val input = Port[Value]

  protected def combineAttributes = span.in combineLatest shift.in map {
    case (sp, sh) => Seq(sp, sh)
  }

  protected def combineInputs = Seq(input.in)

  protected def evaluator(attrs: Seq[Value]) = {
    val spanDuration = (attrs(0): Long) milliseconds
    val shiftDuration = (attrs(1): Long) milliseconds

    _.head.slidingBuffer(spanDuration, shiftDuration) map (_.map(valueToAny).toList)
  }
}