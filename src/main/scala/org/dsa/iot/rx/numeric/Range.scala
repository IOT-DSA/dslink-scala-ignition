package org.dsa.iot.rx.numeric

import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.Having

import rx.lang.scala.Observable

/**
 * Iterates over the specified range of numeric values and emits each item.
 */
class Range[T](implicit num: Numeric[T]) extends AbstractRxBlock[T] {

  def begin(x: T): Range[T] = this having (begin <~ x)
  def end(x: T): Range[T] = this having (end <~ x)
  def step(x: T): Range[T] = this having (step <~ x)

  val begin = Port[T]("begin")
  val end = Port[T]("end")
  val step = Port[T]("step")

  protected def compute = (begin.in combineLatest end.in combineLatest step.in) flatMap {
    case ((start, stop), inc) =>
      val range = Stream.iterate(start)(x => num.plus(x, inc)) takeWhile (x => num.lteq(x, stop))
      Observable.from(range)
  }
}

/**
 * Factory for [[Range]] instances.
 */
object Range {

  /**
   * Creates a new Range instance with begin of 1, end of 10 and step of 1.
   */
  def apply[T](implicit num: Numeric[T]): Range[T] = Range(num.zero, num.fromInt(10), num.one)

  /**
   * Creates a new Range instance with the specified begin and end (both inclusive).
   * The default step of 1 will be used.
   */
  def apply[T](begin: T, end: T)(implicit num: Numeric[T]): Range[T] = apply(begin, end, num.one)

  /**
   * Creates a new Range instance with the specified begin and end (both inclusive) as well as the step.
   */
  def apply[T](begin: T, end: T, step: T)(implicit num: Numeric[T]): Range[T] = {
    val block = new Range[T]
    block.begin <~ begin
    block.end <~ end
    block.step <~ step
    block
  }
}