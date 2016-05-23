package org.dsa.iot.rx.block

import scala.concurrent.duration.DurationLong

import org.dsa.iot.anyToValue
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.valueToLong

import rx.lang.scala.Observable

case class Timer() extends DSARxBlock {
  val delay = Port[Value]

  protected def combineAttributes = delay.in map (Seq(_))

  protected def combineInputs = Nil

  protected def evaluator(attrs: Seq[Value]) = {
    val delay = (attrs.head: Long) milliseconds

    (_: Seq[ValueStream]) => Observable.timer(delay).map(anyToValue)
  }
}