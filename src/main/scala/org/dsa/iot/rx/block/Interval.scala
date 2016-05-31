package org.dsa.iot.rx.block

import scala.concurrent.duration.DurationLong

import org.dsa.iot.anyToValue
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.valueToLong

import rx.lang.scala.Observable

case class Interval() extends DSARxBlock {
  val initial = Port[Value]
  val period = Port[Value]

  protected def combineAttributes = Observable.combineLatest(Seq(initial.in, period.in))(identity)

  protected def combineInputs = Nil

  protected def evaluator(attrs: Seq[Value]) = {
    val initialDuration = (attrs(0): Long) milliseconds
    val periodDuration = (attrs(1): Long) milliseconds

    (_: Seq[ValueStream]) => Observable.interval(initialDuration, periodDuration).map(anyToValue)
  }
}