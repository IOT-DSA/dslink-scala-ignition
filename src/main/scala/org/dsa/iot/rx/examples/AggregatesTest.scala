package org.dsa.iot.rx.examples

import scala.concurrent.duration._

import org.dsa.iot.rx._
import org.dsa.iot.rx.core._
import org.dsa.iot.rx.numeric._

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
  
  testMath

  testCount
  testLength
  testExists
  testIsEmpty
  
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
  
  def testMath() = run("Math") {
    val rng = Sequence.from(1 to 5)
    
    val sum1 = Sum[Int](true)
    sum1.output subscribe testSub("SUM1")
    rng ~> sum1
    
    val sum2 = Sum[Int](false)
    sum2.output subscribe testSub("SUM2")
    rng ~> sum2

    val mul1 = Mul[Int](true)
    mul1.output subscribe testSub("MUL1")
    rng ~> mul1

    val mul2 = Mul[Int](false)
    mul2.output subscribe testSub("MUL2")
    rng ~> mul2
    
    val min1 = Min[Int](true)
    min1.output subscribe testSub("MIN1")
    rng ~> min1

    val min2 = Min[Int](false)
    min2.output subscribe testSub("MIN2")
    rng ~> min2

    val max1 = Max[Int](true)
    max1.output subscribe testSub("MAX1")
    rng ~> max1

    val max2 = Max[Int](false)
    max2.output subscribe testSub("MAX2")
    rng ~> max2
    
    val avg1 = Avg[Int](true)
    avg1.output subscribe testSub("AVG1")
    rng ~> avg1

    val avg2 = Avg[Int](false)
    avg2.output subscribe testSub("AVG2")
    rng ~> avg2
    
    val sts1 = BasicStats[Int](true)
    sts1.output subscribe testSub("STS1")
    rng ~> sts1

    val sts2 = BasicStats[Int](false)
    sts2.output subscribe testSub("STS2")
    rng ~> sts2
    
    rng.reset
  }
  
  def testCount() = run("Count") {
    val rng = Sequence.from(1 to 10)

    val cAll = Count[Int](false)
    cAll.output subscribe testSub("COUNT-ALL")
    rng ~> cAll
    
    val cEven = Count((n: Int) => n % 2 == 0, true)
    cEven.output subscribe testSub("COUNT-EVEN")
    rng ~> cEven
    
    rng.reset
  }
  
  def testLength() = run("Length") {
    val rng = Sequence.from(1 to 5)

    val len1 = Length(false)
    len1.output subscribe testSub("LENGTH1")
    rng ~> len1
    
    val len2 = Length(true)
    len2.output subscribe testSub("LENGTH2")
    rng ~> len2
    
    rng.reset
  }
  
  def testExists() = run("Exists") {
    val rng = Sequence.from(1 to 10)

    val ex = Exists[Int]
    ex.output subscribe testSub("EXISTS")
    rng ~> ex

    ex.predicate <~ ((n: Int) => n % 2 == 0)
    rng.reset

    rng.items <~ (1 to 10 by 2)
    rng.reset
  }
  
  def testIsEmpty() = run("IsEmpty") {
    val rng = Sequence.from(1 to 5)
    
    val emp = IsEmpty()
    emp.output subscribe testSub("EMPTY")
    rng ~> emp

    rng.reset
    
    rng.items <~ Nil
    rng.reset
  }
}