package org.dsa.iot.ignition.core

import org.dsa.iot.ignition.{ RichValue, ScriptDialect }

import com.ignition.rx.RxTransformer

/**
 * Filters input values using a script in the specified dialect.
 */
class ScriptFilter extends RxTransformer[RichValue, RichValue] {
  val dialect = Port[ScriptDialect.ScriptDialect]("dialect")
  val predicate = Port[String]("predicate")

  protected def compute = (dialect.in combineLatest predicate.in) flatMap {
    case (lang, code) => source.in.filter { x =>
      lang.execute[Boolean](code, Map("input" -> x))
    }
  }
}