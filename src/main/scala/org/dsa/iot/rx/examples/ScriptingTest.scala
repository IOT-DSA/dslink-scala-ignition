package org.dsa.iot.rx.examples

import org.dsa.iot.rx.core.Sequence
import org.dsa.iot.rx.script.{ ScriptFilter, ScriptTransform, ScriptCount }
import org.dsa.iot.rx.script.ScriptDialect.{ MVEL, SCALA }

/**
 * Tests scripting blocks.
 */
object ScriptingTest extends TestHarness {

  testMvelTransform
  testScalaTransform

  testMvelFilter
  testScalaFilter
  
  testMvelCount
  testScalaCount

  def testMvelTransform() = run("MvelTransform") {
    val rng = Sequence.from(1 to 5)

    val tx = ScriptTransform[Any, AnyRef](MVEL, "POW(input / 3.0, 2.0)")
    tx.output subscribe testSub("TRANSFORM-MVEL")

    rng ~> tx
    rng.reset
  }

  def testScalaTransform() = run("ScalaTransform") {
    val rng = Sequence.from(1 to 5)

    val tx = ScriptTransform[Any, Any](SCALA, "math.sqrt(input.asInstanceOf[Int])")
    tx.output subscribe testSub("TRANSFORM-SCALA")

    rng ~> tx
    rng.reset
  }

  def testMvelFilter() = run("MvelFilter") {
    val rng = Sequence.from(1 to 5)

    val tx = ScriptFilter[Any](MVEL, "input < 3")
    tx.output subscribe testSub("FILTER-MVEL")

    rng ~> tx
    rng.reset
  }

  def testScalaFilter() = run("ScalaFilter") {
    val rng = Sequence.from(1 to 5)

    val tx = ScriptFilter[Any](SCALA, "val x = input.asInstanceOf[Int]; x > 2.5")
    tx.output subscribe testSub("FILTER-SCALA")

    rng ~> tx
    rng.reset
  }
  
  def testMvelCount() = run("MvelCount") {
    val rng = Sequence.from(1 to 10)
    
    val tx = ScriptCount[Any](MVEL, "input % 3 == 0", true)
    tx.output subscribe testSub("COUNT-MVEL")
    
    rng ~> tx
    rng.reset
  }
  
  def testScalaCount() = run("ScalaCount") {
    val rng = Sequence.from(1 to 10)
    
    val tx = ScriptCount[Any](SCALA, "val x = input.asInstanceOf[Int]; x > 5", false)
    tx.output subscribe testSub("COUNT-SCALA")
    
    rng ~> tx
    rng.reset
  }
}