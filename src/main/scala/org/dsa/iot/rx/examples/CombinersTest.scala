package org.dsa.iot.rx.examples

import scala.concurrent.duration._

import org.dsa.iot.rx._
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

  testMerge

  testZip
  testZip2
  testZip3
  testZip4

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

  def testMerge() = run("Merge") {
    val i1 = Interval(50 milliseconds)

    val i2 = Interval(80 milliseconds, 20 milliseconds)

    val merge = Merge[Long]
    merge.output subscribe testSub("MERGE")

    (i1, i2) ~> merge

    i1.reset
    i2.reset
    delay(300)

    i1.shutdown
    i2.shutdown
  }

  def testZip() = run("Zip") {
    val zip = Zip[Long]
    zip.output subscribe testSub("ZIP")

    val i1 = Interval(100 milliseconds, 50 milliseconds)

    val i2 = Interval(50 milliseconds, 100 milliseconds)

    zip.sources.add(2)
    zip.sources(0) <~ i1
    zip.sources(1) <~ i2
    i1.reset
    i2.reset
    delay(300)

    i2.period <~ (10 milliseconds)
    i2.reset
    delay(500)

    i1.shutdown
    i2.shutdown
  }

  def testZip2() = run("Zip2") {
    val zip = Zip2[Int, String]
    zip.output subscribe testSub("ZIP2")

    val seq1 = Sequence(1, 2, 3)
    val seq2 = Sequence("a", "b", "c", "d")

    zip.source1 <~ seq1
    zip.source2 <~ seq2
    
    seq1.reset
    seq2.reset
  }
  
  def testZip3() = run("Zip3") {
    val zip = Zip3[Long, String, Int]
    zip.output subscribe testSub("ZIP3")

    val i1 = Interval(100 milliseconds)
    val seq2 = Sequence("a", "b", "c", "d")
    val seq3 = Sequence(11, 22, 33, 44, 55, 66)

    zip.source1 <~ i1
    zip.source2 <~ seq2
    zip.source3 <~ seq3
    
    i1.reset
    seq2.reset
    seq3.reset
    
    delay(500)
    
    i1.shutdown
  }
  
  def testZip4() = run("Zip4") {
    val zip = Zip4[Long, Long, Long, Long]
    zip.output subscribe testSub("ZIP4")

    val i1 = Interval(100 milliseconds, 50 milliseconds)
    val i2 = Interval(150 milliseconds, 40 milliseconds)
    val i3 = Interval(120 milliseconds, 20 milliseconds)
    val i4 = Interval(170 milliseconds, 10 milliseconds)

    zip.source1 <~ i1
    zip.source2 <~ i2
    zip.source3 <~ i3
    zip.source4 <~ i4
    
    i1.reset
    i2.reset
    i3.reset
    i4.reset
    
    delay(600)
    
    i1.shutdown
    i2.shutdown
    i3.shutdown
    i4.shutdown
  }
}