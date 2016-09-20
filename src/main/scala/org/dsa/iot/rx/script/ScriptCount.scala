package org.dsa.iot.rx.script

import scala.reflect.runtime.universe.typeTag

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect

/**
 * Counts the number of elements in the source sequence that satisfy the predicate.
 * It can either produce a rolling count on each item, or just one final result when source
 * sequence is complete.
 */
class ScriptCount[T](rolling: Boolean) extends RxTransformer[T, Int] with ScriptedBlock[Boolean] {
  val ttag = typeTag[Boolean]

  protected def compute = scriptStream flatMap { script =>
    if (rolling)
      (source.in filter script.evaluateInput).scan(0)((total, _) => total + 1)
    else
      source.in count script.evaluateInput
  }
}

/**
 * Factory for [[ScriptCount]] instances.
 */
object ScriptCount {

  /**
   * Creates a new ScriptCount instance.
   */
  def apply[T](rolling: Boolean): ScriptCount[T] = new ScriptCount[T](rolling)

  /**
   * Creates a new ScriptCount instance for the specified dialect and predicate code - either as
   * a running total or as a final value only.
   */
  def apply[T](dialect: ScriptDialect, predicate: String, rolling: Boolean): ScriptCount[T] = {
    val block = new ScriptCount[T](rolling)
    block.dialect <~ dialect
    block.script <~ predicate
    block
  }
}