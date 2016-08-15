package org.dsa.iot.ignition.core

import org.dsa.iot.ignition.aio
import com.ignition.rx.AbstractRxBlock
import rx.lang.scala.Observable

/**
 * Generates a range of numbers.
 */
class NumericRange extends AbstractRxBlock[Number] {
  val start = Port[Number]("start")
  val end = Port[Number]("end")
  val step = Port[Number]("step")

  protected def compute = (start.in combineLatest end.in combineLatest step.in) flatMap {
    case ((n1, n2), s) if i(n1) && i(n2) && i(s) => {
      val range = n1.intValue to n2.intValue by s.intValue
      Observable.from(range) map aio[Number]
    }
    case ((d1, d2), s) => {
      val range = Stream.iterate(d1.doubleValue)(_ + s.doubleValue) takeWhile (_ <= d2.doubleValue)
      Observable.from(range) map aio[Number]
    }
  }

  private def i(x: Number) = x.isInstanceOf[Int]
}