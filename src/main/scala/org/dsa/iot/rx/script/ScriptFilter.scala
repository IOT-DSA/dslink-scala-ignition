package org.dsa.iot.rx.script

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect

/**
 * Filters values in the source sequence using a script in the specified dialect.
 */
class ScriptFilter[T] extends RxTransformer[T, T] {
  val dialect = Port[ScriptDialect]("dialect")
  val predicate = Port[String]("predicate")

  protected def compute = (dialect.in combineLatest predicate.in) flatMap {
    case (lang, code) => source.in.filter { x => lang.execute[Boolean](code, Map("input" -> x)) }
  }
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
    block.predicate <~ predicate
    block
  }
}