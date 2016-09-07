package org.dsa.iot.rx.examples

import org.dsa.iot.rx.RichValue
import org.dsa.iot.rx.core._

import rx.lang.scala.Observable

/**
 * Tests producer blocks.
 */
object ProducersTest extends TestHarness {

  testZero
  testValueHolder
  
  def testZero() = run("Zero"){
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

  def testValueHolder() = run("ValueHolder"){
    val vh = ValueHolder[Int]
    vh.output subscribe testSub("VALUE_HOLDER")
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
}