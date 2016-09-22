package org.dsa.iot.rx.script

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect

import scala.reflect.runtime.universe.TypeTag

/**
 * Transforms each value in the source sequence using a script in the specified dialect.
 */
class ScriptTransform[T, R: TypeTag] extends RxTransformer[T, R] with ScriptedBlock[R] {
  val ttag = implicitly[TypeTag[R]]

  protected def compute = scriptStream flatMap { source.in map _.evaluateInput }
}

/**
 * Factory for [[ScriptTransform]] instances.
 */
object ScriptTransform {

  /**
   * Creates a new ScriptTransform instance.
   */
  def apply[T, R: TypeTag]: ScriptTransform[T, R] = new ScriptTransform[T, R]

  /**
   * Creates a new ScriptTransform for the specified dialect and script code.
   */
  def apply[T, R: TypeTag](dialect: ScriptDialect, script: String): ScriptTransform[T, R] = {
    val block = new ScriptTransform[T, R]
    block.dialect <~ dialect
    block.script <~ script
    block
  }
}