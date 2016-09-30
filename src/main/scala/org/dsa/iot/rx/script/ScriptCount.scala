package org.dsa.iot.rx.script

import scala.reflect.runtime.universe.typeTag

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect
import org.dsa.iot.scala.Having

/**
 * Counts the number of elements in the source sequence that satisfy the predicate.
 * It can either produce a rolling count on each item, or just one final result when source
 * sequence is complete.
 */
class ScriptCount[T] extends RxTransformer[T, Int] with ScriptedBlock[Boolean] {
  val ttag = typeTag[Boolean]

  def running(): ScriptCount[T] = this having (rolling <~ true)
  def single(): ScriptCount[T] = this having (rolling <~ false)

  val rolling = Port[Boolean]("rolling")

  protected def compute = scriptStream combineLatest rolling.in flatMap {
    case (script, true)  => (source.in filter script.evaluateInput).scan(0)((total, _) => total + 1)
    case (script, false) => source.in count script.evaluateInput
  }
}

/**
 * Factory for [[ScriptCount]] instances.
 */
object ScriptCount {

  /**
   * Creates a new ScriptCount instance.
   */
  def apply[T]: ScriptCount[T] = new ScriptCount[T]

  /**
   * Creates a new ScriptCount instance for the specified dialect and predicate code - either as
   * a running total or as a final value only.
   */
  def apply[T](dialect: ScriptDialect, predicate: String, rolling: Boolean): ScriptCount[T] = {
    val block = new ScriptCount[T]
    block.rolling <~ rolling
    block.dialect <~ dialect
    block.script <~ predicate
    block
  }
}