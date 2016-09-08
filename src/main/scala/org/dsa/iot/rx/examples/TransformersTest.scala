package org.dsa.iot.rx.examples

import scala.concurrent.duration._

import org.dsa.iot.rx._
import org.dsa.iot.rx.core._

import rx.lang.scala.Observable

/**
 * Tests transformer blocks.
 */
object TransformersTest extends TestHarness {

  testZipWithIndex

  testWindowBySize
  testWindowByTime

  def testZipWithIndex() = run("ZipWithIndex") {
    val zi = ZipWithIndex[String]
    zi.output subscribe testSub("ZIP-INDEX")

    val seq = Sequence("a", "b", "c", "d", "e")
    seq ~> zi

    seq.reset
  }

  def testWindowBySize() = run("WindowBySize") {
    val w1 = WindowBySize[Int](3)
    w1.output subscribe testSub("WIN-SIZE1")

    val seq = Sequence.from(1 to 8)
    seq ~> w1
    seq.reset

    val w2 = WindowBySize[Long](5, 2)
    w2.output subscribe testSub("WIN-SIZE2")
    
    val i = Interval(50 milliseconds, 20 milliseconds)
    i ~> w2
    i.reset
    
    delay(500)
    i.shutdown
  }
  
  def testWindowByTime() = run("WindowByTime") {
    val w1 = WindowByTime[Long](200 milliseconds)
    w1.output subscribe testSub("WIN-TIME1")
    
    val i = Interval(40 milliseconds, 20 milliseconds)
    i ~> w1
    i.reset
    
    delay(500)
    i.shutdown
  }
}