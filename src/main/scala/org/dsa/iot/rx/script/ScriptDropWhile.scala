package org.dsa.iot.rx.script

import scala.reflect.runtime.universe.typeTag
import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect

/**
 * Drops items from the source as long as the predicate condition in the specified dialect is true.
 */
class ScriptDropWhile[T] extends RxTransformer[T, T] with ScriptedBlock[Boolean] {
  val ttag = typeTag[Boolean]

  protected def compute = scriptStream flatMap { source.in dropWhile _.evaluateInput }
}

/**
 * Factory for [[ScriptDropWhile]] instances.
 */
object ScriptDropWhile {

  /**
   * Creates a new ScriptDropWhile instance.
   */
  def apply[T]: ScriptDropWhile[T] = new ScriptDropWhile[T]

  /**
   * Creates a new ScriptDropWhile instance for the specified dialect and predicate code.
   */
  def apply[T](dialect: ScriptDialect, predicate: String): ScriptDropWhile[T] = {
    val block = new ScriptDropWhile[T]
    block.dialect <~ dialect
    block.script <~ predicate
    block
  }
}