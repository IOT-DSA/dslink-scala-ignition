package org.dsa.iot.rx.script

import scala.reflect.runtime.universe.typeTag

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect

/**
 * Tests whether a predicate in the specified dialect holds for some of the elements of the source.
 */
class ScriptExists[T] extends RxTransformer[T, Boolean] with ScriptedBlock[Boolean] {
  val ttag = typeTag[Boolean]

  protected def compute = scriptStream flatMap { source.in exists _.evaluateInput }
}

/**
 * Factory for [[ScriptExists]] instances.
 */
object ScriptExists {

  /**
   * Creates a new ScriptExists instance.
   */
  def apply[T]: ScriptExists[T] = new ScriptExists[T]

  /**
   * Creates a new ScriptExists instance for the specified dialect and predicate code.
   */
  def apply[T](dialect: ScriptDialect, predicate: String): ScriptExists[T] = {
    val block = new ScriptExists[T]
    block.dialect <~ dialect
    block.script <~ predicate
    block
  }
}