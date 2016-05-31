package org.dsa.iot.rx

import rx.lang.scala._
import rx.lang.scala.subjects._
import concurrent.duration._

/**
 * 0 attributes, 0 inputs.
 */
case class One() extends AbstractRxBlock[Unit, Unit, Int] {
  protected def combineAttributes = Observable.just({})
  protected def combineInputs = {}
  protected def evaluator(attrs: Unit) = (_: Unit) => Observable.just(1)
}

/**
 * 1 attribute, 0 inputs.
 */
case class Just[A]() extends AbstractRxBlock[A, Unit, A] {

  val value = Port[A]

  protected def combineAttributes = value.in
  protected def combineInputs = {}
  protected def evaluator(item: A) = (_: Unit) => Observable.just(item)
}

/**
 * 2 attributes, 0 inputs.
 */
case class Interval() extends AbstractRxBlock[(Duration, Duration), Unit, Long] {

  val initial = Port[Duration]
  val period = Port[Duration]

  protected def combineAttributes = initial.in combineLatest period.in
  protected def combineInputs = {}
  protected def evaluator(attrs: (Duration, Duration)) = attrs match {
    case (initial, period) => (_: Unit) => Observable.interval(initial, period)
  }
}

/**
 * 0 attributes, 1 input.
 */
case class First[R]() extends AbstractRxBlock[Unit, Observable[R], R] {

  val source = Port[R]

  protected def combineAttributes = Observable.just({})
  protected def combineInputs = source.in
  protected def evaluator(attrs: Unit) = _.first
}

/**
 * 1 attribute, 1 input.
 */
case class Take[R]() extends AbstractRxBlock[Int, Observable[R], R] {

  val size = Port[Int]
  val source = Port[R]

  protected def combineAttributes = size.in
  protected def combineInputs = source.in
  protected def evaluator(size: Int) = _.take(size)
}

/**
 * 2 attributes, 1 input.
 */
case class SlidingWindow[R]() extends AbstractRxBlock[(Int, Int), Observable[R], Seq[R]] {

  val count = Port[Int]
  val skip = Port[Int]
  val source = Port[R]

  protected def combineAttributes = count.in combineLatest skip.in
  protected def combineInputs = source.in
  protected def evaluator(attrs: (Int, Int)) = _.slidingBuffer(attrs._1, attrs._2)
}

/**
 * 0 attributes, 3 inputs.
 */
case class Zip[R1, R2, R3]() extends AbstractRxBlock[Unit, (Observable[R1], Observable[R2], Observable[R3]), (R1, R2, R3)] {

  val source1 = Port[R1]
  val source2 = Port[R2]
  val source3 = Port[R3]

  protected def combineAttributes = Observable.just({})
  protected def combineInputs = (source1.in, source2.in, source3.in)
  protected def evaluator(attrs: Unit) = {
    case (i1, i2, i3) => Observable.zip(i1, i2, i3)
  }
}

/**
 * Block testing.
 */
object RxTest extends App with Logging {

  testZip
  
  def testZip() = {
    val interval1 = Interval()
    interval1.initial.set(0 milliseconds)
    interval1.period.set(200 milliseconds)

    val interval2 = Interval()
    interval2.initial.set(50 milliseconds)
    interval2.period.set(50 milliseconds)

    val interval3 = Interval()
    interval3.initial.set(200 milliseconds)
    interval3.period.set(100 milliseconds)
    
    val zip = Zip[Long, Long, Long]
    zip.output subscribe testSub("ZIP")
    zip.source1.bind(interval1)
    zip.source2.bind(interval2)
    zip.source3.bind(interval3)
    
    delay(4 seconds)
    
    val just1 = Just[Long]
    just1.value.set(111)
    zip.source2.bind(just1)
    delay(2 seconds)

    val just2 = Just[Long]
    just2.value.set(333)
    zip.source1.bind(just2)
    delay(2 seconds)
  }

  def testSlidingWindow() = {
    val interval = Interval()
    interval.initial.set(0 milliseconds)
    interval.period.set(200 milliseconds)

    val window = SlidingWindow[Long]
    window.output subscribe testSub("WINDOW")
    window.count.set(10)
    window.skip.set(3)
    window.source.bind(interval)
    delay(3 seconds)

    window.skip.set(5)
    delay(3 seconds)

    window.reset
    delay(3 seconds)
  }

  def testTake() = {
    val interval = Interval()
    interval.initial.set(0 milliseconds)
    interval.period.set(200 milliseconds)

    val take = Take[Long]
    take.output subscribe testSub("TAKE")
    take.size.set(5)
    take.source.bind(interval)
    delay(2 seconds)

    take.reset
    delay(1 second)

    take.size.set(10)
    delay(3 seconds)
  }

  def testFirst() = {
    val interval = Interval()
    interval.initial.set(0 milliseconds)
    interval.period.set(200 milliseconds)

    val first = First[Long]()
    first.output subscribe testSub("FIRST")
    first.source.bind(interval)
    delay(2 second)

    first.reset
    delay(1 seconds)

    val just = Just[Long]()
    first.source.bind(just)
    just.value.set(555)
    delay(1 second)

    just.value.set(333)
    first.source.bind(just)
    delay(1 second)
  }

  def testInterval() = {
    val interval = Interval()
    interval.output subscribe testSub("INTERVAL")

    interval.initial.set(50 milliseconds)
    interval.period.set(200 milliseconds)
    delay(1 second)

    interval.period.set(100 milliseconds)
    delay(1 second)
  }

  def testJust() = {
    val just = Just[Int]()
    just.output subscribe testSub("JUST")
    just.value.set(123)
    just.value.set(456)
    just.value.set(789)
    delay(500 milliseconds)

    val one = One()
    one.reset
    just.value.bind(one)
    delay(500 milliseconds)
  }

  def testOne() = {
    val one = One()
    one.output subscribe testSub("ONE")
    one.reset
    delay(500 milliseconds)
    one.reset
    delay(500 milliseconds)
  }

  //  testJoin
  //  
  //  def testJoin() = {
  //    val join = Join[Int, String]()
  //    join.output subscribe testSub("JOIN")
  //    
  //    join.setSrc1(Observable.interval(200 milliseconds).map(_.toInt).take(5))
  //    join.setSrc2(Observable.interval(200 milliseconds).map(_.toString))
  //    
  //    delay(2 seconds)
  //    
  //    join.setSrc2(Observable.interval(200 milliseconds).map(x => (x * 3).toString))
  //    
  //    delay(2 seconds)
  //  }
  //  
  //  def testFilter() = {
  //    val filter = Filter[Long]()
  //    filter.output subscribe testSub("FILTER")
  //    
  //    filter.setSrc(Observable.interval(200 milliseconds))
  //    filter.setPredicate(Observable.just((x: Long) => (x % 3 == 0)))
  //    
  //    delay(3 seconds)
  //    
  //    filter.setPredicate(Observable.interval(2 seconds).drop(2).map(y => (x: Long) => (x % y == 0)))
  //    delay(15 seconds)
  //  }
  //  
  //  def testCombine() = {
  //    val comb = Combine[java.lang.Integer, java.lang.Integer]()
  //    comb.output subscribe testSub("COMB")
  //
  //    comb.setSrc1(Observable.interval(200 milliseconds).map(_.toInt))
  //    comb.setSrc2(Observable.interval(300 milliseconds).take(5).map(_.toInt).map(x => Seq(x, x, x)))
  //    comb.setOp1(Observable.just("*"))
  //    comb.setOp2(Observable.just("+"))
  //
  //    delay(3 seconds)
  //
  //    comb.reset
  //
  //    delay(2 seconds)
  //
  //    comb.setOp2(Observable.just("-"))
  //
  //    delay(2 seconds)
  //
  //    comb.setOp2(Observable.just("*"))
  //    delay(1 seconds)
  //    comb.setSrc1(Observable.interval(200 milliseconds).map(_.toInt))
  //
  //    delay(3 seconds)
  //  }

  //  testHead
  //
  //  def testHead() = {
  //    info("Creating first Interval")
  //    val b11 = Interval()
  //    b11.initial.set(100 milliseconds)
  //    b11.period.set(200 milliseconds)
  //
  //    info("Creating second Interval")
  //    val b12 = Interval()
  //    b12.initial.set(100 milliseconds)
  //    b12.period.set(200 milliseconds)
  //
  //    info("Creating Head")
  //    val b2 = Head[Long]()
  //    info("Binding Head to test sub")
  //    b2.output subscribe testSub("B21")
  //
  //    b2.input.bind(b11)
  //    delay(1 seconds)
  //    b2.input.bind(b12)
  //    delay(2 seconds)
  //  }
  //
  //  def testSeqWithDelay() = {
  //    val b1 = SeqWithDelay[String]
  //    b1.output subscribe testSub("B1")
  //    b1.items.set(Seq("A", "B", "C"))
  //    b1.period.set(200 milliseconds)
  //    b1.initial.set(100 milliseconds)
  //
  //    delay(2 seconds)
  //  }
  //
  //  def testFromInterval() = {
  //    val b1 = From[Duration]
  //    b1.items.set(Seq(500 milliseconds))
  //
  //    val b2 = Interval()
  //    b2.output subscribe testSub("B2")
  //
  //    b2.initial.set(50 milliseconds)
  //    b2.period.bind(b1)
  //
  //    b1.reset()
  //    delay(2 seconds)
  //  }
  //
  //  def testInterval() = {
  //    val b1 = Interval()
  //    b1.output subscribe testSub("B1")
  //    b1.initial.set(100 milliseconds)
  //    delay(1 second)
  //    b1.period.set(200 milliseconds)
  //    delay(1 second)
  //    b1.period.set(1000 milliseconds)
  //    delay(3 seconds)
  //    b1.period.set(100 milliseconds)
  //    delay(1 second)
  //    b1.reset
  //    delay(1 seconds)
  //    b1.reset
  //    delay(1 seconds)
  //  }
  //
  //  def testFrom() = {
  //    val b1 = From[Int]()
  //    b1.output subscribe testSub("B1")
  //    b1.items.set(Seq(1, 3, 5, 7))
  //    b1.reset()
  //    b1.items.set(Seq(2, 3))
  //    b1.reset()
  //
  //    delay(1 second)
  //  }
  //
  //  def testFixedSeq() = {
  //    val b1 = FixedSeq()
  //    b1.output subscribe testSub("B1")
  //    b1.reset
  //    delay(2 seconds)
  //    b1.reset
  //    delay(6 seconds)
  //    b1.reset
  //    delay(2 seconds)
  //    b1.reset
  //  }

  def testSub[T](name: String) = Subscriber[T](
    (x: T) => info(s"$name: $x"),
    (err: Throwable) => error(s"$name: $err", err),
    () => info(s"$name: done"))

  def delay(duration: Duration) = Thread.sleep(duration.toMillis)
}