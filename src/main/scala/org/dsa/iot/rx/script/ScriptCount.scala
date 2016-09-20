package org.dsa.iot.rx.script

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect

/**
 * Counts the number of elements in the source sequence that satisfy the predicate.
 * It can either produce a rolling count on each item, or just one final result when source
 * sequence is complete.
 */
class ScriptCount[T](rolling: Boolean) extends RxTransformer[T, Int] {
  val dialect = Port[ScriptDialect]("dialect")
  val predicate = Port[String]("predicate")

  protected def compute = (dialect.in combineLatest predicate.in) flatMap {
    case (lang, code) if rolling => source.in.filter { x =>
      lang.execute[Boolean](code, Map("input" -> x))
    }.scan(0)((total, _) => total + 1)
    case (lang, code) if !rolling => source.in.count {
      x => lang.execute[Boolean](code, Map("input" -> x))
    }
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
    block.predicate <~ predicate
    block
  }
}