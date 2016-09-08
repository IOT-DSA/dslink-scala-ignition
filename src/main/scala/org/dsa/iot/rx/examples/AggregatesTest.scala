package org.dsa.iot.rx.examples

import scala.concurrent.duration._

import org.dsa.iot.rx._
import org.dsa.iot.rx.core._

import rx.lang.scala.Observable

/**
 * Tests aggregator blocks.
 */
object AggregatesTest extends TestHarness {

  testFirst
  testLast

  testScan
  testFold
  testReduce

  def testFirst() = run("First") {
    val rng = Sequence.from(1 to 10)

    val first = First[Int]
    first.output subscribe testSub("FIRST")
    rng ~> first

    rng.reset

    val emp = Empty()
    first.default <~ Some(99)
    emp ~> first

    emp.reset
  }

  def testLast() = run("Last") {
    val rng = Sequence.from(1 to 10)

    val last = Last[Int]
    last.output subscribe testSub("LAST")
    rng ~> last

    rng.reset

    val emp = Empty()
    last.default <~ Some(99)
    emp ~> last

    emp.reset
  }

  def testScan() = run("Scan") {
    val rng = Sequence.from(1 to 5)

    val scan = Scan(0, (a: Int, b: Int) => a + b)
    scan.output subscribe testSub("SCAN")

    rng ~> scan
    rng.reset
  }

  def testFold() = run("Fold") {
    val rng = Sequence.from(1 to 5)

    val fold = Fold("data", (s: String, n: Int) => s ++ ":" + n.toString)
    fold.output subscribe testSub("FOLD")
    rng ~> fold

    rng.reset

    rng.items <~ Seq(1, 2, 4, 8)
    rng.reset
  }

  def testReduce() = run("Reduce") {
    val rng = Sequence.from(1 to 5)

    val red = Reduce((a: Int, b: Int) => a * b)
    red.output subscribe testSub("REDUCE")
    rng ~> red

    rng.reset
  }
}