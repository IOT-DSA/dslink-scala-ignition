package org.dsa.iot.rx.script

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect

import scala.reflect.runtime.universe.TypeTag

/**
 * Repeatedly applies a scripted function, where the first argument is the result obtained in the previous
 * application and the second argument is each element of the source sequence.
 */
class ScriptReduce[T: TypeTag] extends RxTransformer[T, T] with ScriptedBlock[T] {
  val ttag = implicitly[TypeTag[T]]

  protected def compute = scriptStream flatMap { source.in reduce _.evaluateInputs2 }
}

/**
 * Factory for [[ScriptReduce]] instances.
 */
object ScriptReduce {

  /**
   * Creates a new ScriptReduce instance.
   */
  def apply[T: TypeTag]: ScriptReduce[T] = new ScriptReduce[T]

  /**
   * Creates a new ScriptReduce for the specified dialect and script code.
   */
  def apply[T: TypeTag](dialect: ScriptDialect, script: String): ScriptReduce[T] = {
    val block = new ScriptReduce[T]
    block.dialect <~ dialect
    block.script <~ script
    block
  }
}