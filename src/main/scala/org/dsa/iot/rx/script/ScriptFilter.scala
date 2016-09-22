package org.dsa.iot.rx.script

import scala.reflect.runtime.universe.typeTag

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect

/**
 * Filters values in the source sequence using a script in the specified dialect.
 */
class ScriptFilter[T] extends RxTransformer[T, T] with ScriptedBlock[Boolean] {
  val ttag = typeTag[Boolean]

  protected def compute = scriptStream flatMap { source.in filter _.evaluateInput }
}

/**
 * Factory for [[ScriptFilter]] instances.
 */
object ScriptFilter {

  /**
   * Creates a new ScriptFilter instance.
   */
  def apply[T]: ScriptFilter[T] = new ScriptFilter[T]

  /**
   * Creates a new ScriptFilter instance for the specified dialect and predicate code.
   */
  def apply[T](dialect: ScriptDialect, predicate: String): ScriptFilter[T] = {
    val block = new ScriptFilter[T]
    block.dialect <~ dialect
    block.script <~ predicate
    block
  }
}