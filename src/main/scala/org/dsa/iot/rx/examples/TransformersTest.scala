package org.dsa.iot.rx.examples

import scala.concurrent.duration._
import org.dsa.iot.rx._
import org.dsa.iot.rx.core._
import rx.lang.scala.Observable
import scala.util.control.NonFatal

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

  testDropByTime
  testDropByCount
  testDropWhile

  testDebounce
  testDelay
  testDistinct
  testSample
  testRepeat

  testTransform
  testCollect

  testCache

  testContains
  testElementAt
  testFilter

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

  def testDropByTime() = run("DropByTime") {
    val i1 = Interval(100 milliseconds)

    val drop = DropByTime[Long](200 milliseconds, false)
    drop.output subscribe testSub("DROP-TIME")
    drop.source <~ i1

    i1.reset
    delay(500)

    i1.period <~ (50 milliseconds)
    i1.reset
    delay(400)
    i1.shutdown
  }

  def testDropByCount() = run("DropByCount") {
    val rng = Sequence.from(1 to 10)

    val drop = DropByCount[Int](4, false)
    drop.output subscribe testSub("DROP-COUNT")
    drop.source <~ rng

    rng.reset
  }

  def testDropWhile() = run("DropWhile") {
    val rng = Sequence.from(1 to 10)

    val drop = DropWhile[Int]
    drop.output subscribe testSub("DROP-WHILE")

    drop.predicate <~ ((n: Int) => n < 5)
    drop.source <~ rng

    rng.reset
  }

  def testDebounce() = run("Debounce") {
    val i = RandomInterval(50 milliseconds, 100 milliseconds)

    val deb = Debounce[Long](75 milliseconds)
    deb.output subscribe testSub("DEBOUNCE")
    i ~> deb

    i.reset
    delay(800)
    i.shutdown
  }

  def testDelay() = run("Delay") {
    val rng = Sequence.from(1 to 15)

    val del = Delay[Int](500 milliseconds)
    del.output subscribe testSub("DELAY")
    del.source <~ rng

    rng.reset
    delay(600)
  }

  def testDistinct() = run("Distinct") {
    val rng = Sequence.from(1 to 20)

    val dis = Distinct[Int](true, (n: Int) => n / 2)
    dis.output subscribe testSub("DISTINCT")

    dis.source <~ rng

    rng.reset

    dis.selector <~ ((n: Int) => n % 3)
    rng.reset

    dis.global <~ false
    dis.selector <~ ((n: Int) => n)
    rng.items <~ Seq(1, 1, 1, 2, 3, 4, 4)
    rng.reset
  }

  def testSample() = run("Sample") {
    val i = Interval(30 milliseconds)

    val smp = Sample[Long](100 milliseconds)
    smp.output subscribe testSub("SAMPLE")
    i ~> smp

    i.reset
    delay(350)

    i.shutdown
  }

  def testRepeat() = run("Repeat") {
    val rng = Sequence.from(1 to 4)

    val rep = Repeat[Int](3)
    rep.output subscribe testSub("REPEAT")
    rng ~> rep

    rng.reset
  }

  def testTransform() = run("Transform") {
    val rng = Sequence.from(1 to 3)

    val tx = Transform[Int]
    tx.output subscribe testSub("TRANSFORM")
    rng ~> tx

    rng.reset

    tx.operator <~ ((n: Int) => n * 2)
    rng.reset
  }

  def testCollect() = run("Collect") {
    val i1 = Interval(100 milliseconds)
    i1.reset
    delay(300)

    val collect = Collect[Long, String]
    collect.output subscribe testSub("COLLECT")

    i1 ~> collect.source

    val even: PartialFunction[Long, String] = {
      case x if x % 2 == 0 => s"$x: even"
    }

    val mul: PartialFunction[Long, String] = {
      case x if x % 3 == 0 => s"$x: *3"
      case x if x % 4 == 0 => s"$x: *4"
      case x if x % 5 == 0 => s"$x: *5"
    }

    collect.selector <~ even
    collect.reset
    delay(500)

    collect.selector <~ mul
    collect.reset
    delay(800)

    i1.reset
    delay(500)

    i1.shutdown
  }

  def testCache() = run("Cache") {
    val i = Interval(50 milliseconds)
    val cache = Cache[Long]
    i ~> cache

    val tx1 = Transform((n: Long) => ">" + n.toString)
    tx1.output subscribe testSub("CACHE1")
    cache ~> tx1

    i.reset
    delay(200)

    val tx2 = Transform((n: Long) => "<" + n.toString)
    tx2.output subscribe testSub("CACHE2")
    cache ~> tx2
    tx2.reset

    delay(200)

    i.shutdown
  }

  def testContains() = run("Contains") {
    val i = Interval(50 milliseconds)
    val take = TakeByCount[Long](5)
    take.output subscribe testSub("TAKE1-5")
    i ~> take

    val c3 = Contains[Long](3)
    c3.output subscribe testSub("CONTAINS-3")
    take ~> c3

    val c7 = Contains[Long](7)
    c7.output subscribe testSub("CONTAINS-7")
    take ~> c7

    i.reset
    delay(300)

    i.shutdown
  }

  def testElementAt() = run("ElementAt") {
    val rng = Sequence.from(0 to 5)

    val ea = ElementAt(4, 99)
    ea.output subscribe testSub("ELEMENT")
    rng ~> ea

    rng.reset

    ea.index <~ 10
    rng.reset

    try {
      ea.default <~ None
      rng.reset
    } catch {
      case NonFatal(e) => error("Index out of bounds")
    }
  }
  
  def testFilter() = run("Filter") {
    val rng = Sequence.from(1 to 10)

    val filter = Filter((n: Int) => n > 3 && n < 7)
    filter.output subscribe testSub("FILTER")
    rng ~> filter

    rng.reset
  }
}