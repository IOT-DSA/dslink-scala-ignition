package org.dsa.iot.ignition.core

import scala.reflect.runtime.universe

import org.dsa.iot.anyToValue
import org.dsa.iot.ignition.{ RichValue, ScriptDialect }

import com.ignition.rx.RxTransformer

/**
 * Transforms each value in the observable using a script.
 */
class ScriptMap extends RxTransformer[RichValue, RichValue] {
  val dialect = Port[ScriptDialect.ScriptDialect]("dialect")
  val script = Port[String]("script")

  protected def compute = (dialect.in combineLatest script.in) flatMap {
    case (lang, code) => source.in.map { x =>
      anyToValue(lang.execute[Object](code, Map("input" -> x)))
    }
  }
}