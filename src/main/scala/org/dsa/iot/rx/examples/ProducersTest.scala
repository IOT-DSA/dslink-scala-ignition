package org.dsa.iot.rx.examples

import scala.concurrent.duration._

import org.dsa.iot.rx.RichValue
import org.dsa.iot.rx.core._

import rx.lang.scala.Observable

/**
 * Tests producer blocks.
 */
object ProducersTest extends TestHarness {

  testZero
  testValueHolder

  testFromList
  testSequence
  
  testInterval
  testTimer
  testRandomInterval

  def testZero() = run("Zero") {
    val zero1 = Zero[Int]
    zero1.output subscribe testSub("ZERO-INT")
    zero1.reset
    delay(100)
    zero1.reset
    delay(100)

    val zero2 = Zero[BigDecimal]
    zero2.output subscribe testSub("ZERO-BD")
    zero2.reset
    delay(100)
    zero2.reset

    val zero3 = Zero[Point]
    zero3.output subscribe testSub("ZERO-PNT")
    zero3.reset
    delay(100)
    zero3.reset
  }

  def testValueHolder() = run("ValueHolder") {
    val vh = ValueHolder[Int]
    vh.output subscribe testSub("VALUE-HOLDER")
    vh.value <~ 100
    200 ~> vh.value
    vh.reset

    val vh2 = ValueHolder(300)
    vh.value <~ vh2
    vh2.reset
    400 ~> vh2.value
    vh2.reset

    vh2.value.set(500)
    vh2.reset

    val vh3 = ValueHolder(Observable.from(List(1, 2, 3)))
    vh.value <~ vh3
    vh3.reset
  }

  def testInterval() = run("Interval") {
    val interval = Interval(100 milliseconds, 50 milliseconds)
    interval.output subscribe testSub("INTERVAL")

    interval.reset
    delay(500)

    interval.period.set(200 milliseconds)
    interval.reset
    delay(400)

    val vh = ValueHolder(150 milliseconds)
    vh ~> interval.period
    vh.reset
    delay(400)

    interval.shutdown
  }

  def testFromList() = run("FromList") {
    val seq = FromList[String]("abc", "xyz")
    seq.output subscribe testSub("FROM-LIST")

    seq.reset
    delay(50)
    seq.items.add <~ "zzz"
    seq.items(1) <~ "123"
    seq.reset
    delay(50)
    seq.items.removeLast
    seq.reset
    delay(50)
  }

  def testSequence() = run("Sequence") {
    val seq = Sequence[Int]
    seq.output subscribe testSub("SEQUENCE-INT")

    seq.items <~ (5 to 20 by 4)
    seq.reset
    
    val seq2 = Sequence("a", "b", "c")
    seq2.output subscribe testSub("SEQUENCE-STRING")
    seq2.reset
  }

  def testTimer() = run("Timer") {
    val timer = Timer(100 milliseconds)
    timer.output subscribe testSub("TIMER")

    timer.reset
    delay(200)

    val vh = ValueHolder(200 milliseconds)
    vh ~> timer.delay
    vh.reset
    delay(300)
  }
  
  def testRandomInterval() = run("RandomInterval") {
    val i1 = RandomInterval(50 milliseconds, 200 milliseconds, false)
    i1.output subscribe testSub("RANDOM-INTERVAL1")
    
    i1.reset
    delay(600)
    i1.shutdown
    
    val i2 = RandomInterval(50 milliseconds, 200 milliseconds, true)
    i2.output subscribe testSub("RANDOM-INTERVAL2")
    
    i2.reset
    delay(600)
    i2.shutdown
  }
}