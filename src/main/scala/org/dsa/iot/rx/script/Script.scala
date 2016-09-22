package org.dsa.iot.rx.script

import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect

import scala.reflect.runtime.universe.TypeTag

/**
 * Evaluates the supplied data and returns the result.
 */
trait Script[U] {

  /**
   * Evaluates the context.
   */
  def evaluate(context: Map[String, Any]): U

  /**
   * Evaluates a single input, passing it to the context under name `input`.
   */
  def evaluateInput[T](x: T) = evaluate(Map("input" -> x))

  /**
   * Evaluates two inputs, passing them to the context under names `input1` and `input2`.
   */
  def evaluateInputs2[T1, T2](x1: T1, x2: T2) = evaluate(Map("input1" -> x1, "input2" -> x2))

  /**
   * Evaluates three inputs, passing them to the context under names `input1`, `input2` and `input3`.
   */
  def evaluateInputs3[T1, T2, T3](x1: T1, x2: T2, x3: T3) = evaluate(Map("input1" -> x1, "input2" -> x2, "input3" -> x3))
}

/**
 * The default implementation of [[Script]] that uses a [[ScriptDialect]] and code.
 */
class DefaultScript[U: TypeTag](lang: ScriptDialect, code: String) extends Script[U] {
  def evaluate(context: Map[String, Any]): U = lang.execute[U](code, context)
}