package org.dsa.iot.rx.script

import scala.reflect.runtime.universe.typeTag
import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect

/**
 * Emits items from the source as long as the predicate condition in the specified dialect is true.
 */
class ScriptTakeWhile[T] extends RxTransformer[T, T] with ScriptedBlock[Boolean] {
  val ttag = typeTag[Boolean]

  protected def compute = scriptStream flatMap { source.in takeWhile _.evaluateInput }
}

/**
 * Factory for [[ScriptTakeWhile]] instances.
 */
object ScriptTakeWhile {

  /**
   * Creates a new ScriptTakeWhile instance.
   */
  def apply[T]: ScriptTakeWhile[T] = new ScriptTakeWhile[T]

  /**
   * Creates a new ScriptTakeWhile instance for the specified dialect and predicate code.
   */
  def apply[T](dialect: ScriptDialect, predicate: String): ScriptTakeWhile[T] = {
    val block = new ScriptTakeWhile[T]
    block.dialect <~ dialect
    block.script <~ predicate
    block
  }
}