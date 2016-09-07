package org.dsa.iot.rx.examples

import scala.concurrent.duration._

import org.dsa.iot.rx.RichValue
import org.dsa.iot.rx.core._

import rx.lang.scala.Observable

/**
 * Tests combiner blocks.
 */
object CombinersTest extends TestHarness {

  testAMB

  testCombineLatest
  testCombineLatest2
  testCombineLatest3

  def testAMB() = run("AMB") {
    val amb = AMB[Long]
    amb.output subscribe testSub("AMB")

    val i1 = Interval(50 milliseconds, 200 milliseconds)

    val i2 = Interval(200 milliseconds, 100 milliseconds)

    val i3 = Interval(400 milliseconds, 300 milliseconds)

    amb.sources.add(3)
    i1 ~> amb.sources(0)
    i2 ~> amb.sources(1)
    i3 ~> amb.sources(2)

    i1.reset
    i2.reset
    i3.reset
    delay(500)

    i1.shutdown
    i2.shutdown
    i3.shutdown
  }

  def testCombineLatest() = run("CombineLatest") {
    val cmb = CombineLatest[Long]
    cmb.output subscribe testSub("COMBINE")

    val i1 = Interval(50 milliseconds)

    val vh = ValueHolder(55L)

    cmb.sources.add(2)
    cmb.sources(0) <~ i1
    cmb.sources(1) <~ vh

    i1.reset
    vh.reset
    delay(100)

    vh.value <~ 66
    vh.reset
    delay(100)

    i1.period <~ (20 milliseconds)
    i1.reset
    delay(100)
    
    i1.shutdown
    cmb.shutdown
  }

  def testCombineLatest2() = run("CombineLatest2") {
    val cmb = CombineLatest2[Int, String]
    cmb.output subscribe testSub("COMBINE2")

    cmb.source1 <~ 100
    cmb.source2 <~ "hello"
    cmb.reset
    delay(100)

    cmb.source2 <~ "world"
    cmb.reset
    delay(100)
    
    cmb.shutdown
  }

  def testCombineLatest3() = run("CombineLatest3") {
    val cmb = CombineLatest3[Long, String, Boolean]
    cmb.output subscribe testSub("COMBINE3")

    val i1 = Interval(100 milliseconds, 50 milliseconds)

    cmb.source1 <~ i1
    cmb.source2 <~ "hello"
    cmb.source3 <~ true
    i1.reset
    delay(200)

    cmb.source2 <~ "world"
    cmb.reset
    delay(200)

    cmb.source3 <~ false
    cmb.reset
    delay(100)
    
    i1.shutdown
    cmb.shutdown
  }
}