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
}