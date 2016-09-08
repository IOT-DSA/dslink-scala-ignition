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

  testTakeByTime
  testTakeByCount
  testTakeRight
  testTakeWhile

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
    w1.shutdown
  }

  def testTakeByTime() = run("TakeByTime") {
    val i1 = Interval(100 milliseconds, 50 milliseconds)

    val take = TakeByTime[Long](200 milliseconds)
    take.output subscribe testSub("TAKE-TIME")
    take.source <~ i1

    i1.reset
    delay(500)

    i1.period <~ (50 milliseconds)
    i1.reset
    delay(400)

    i1.shutdown
  }

  def testTakeByCount() = run("TakeByCount") {
    val rng = Sequence.from(1 to 10)

    val take = TakeByCount[Int](4)
    take.output subscribe testSub("TAKE-COUNT")
    take.source <~ rng

    rng.reset
  }

  def testTakeRight() = run("TakeRight") {
    val rng = Sequence.from(1 to 10)

    val take = TakeRight[Int](3)
    take.output subscribe testSub("TAKE-RIGHT")
    take.source <~ rng

    rng.reset
  }

  def testTakeWhile() = run("TakeWhile") {
    val rng = Sequence.from(1 to 10)

    val take = TakeWhile[Int]
    take.output subscribe testSub("TAKE-WHILE")
    take.predicate <~ ((n: Int) => n < 5)
    take.source <~ rng

    rng.reset
  }
}