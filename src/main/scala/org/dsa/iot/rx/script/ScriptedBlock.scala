package org.dsa.iot.rx.script

import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.rx.script.ScriptDialect.ScriptDialect
import org.dsa.iot.scala.Having

import rx.lang.scala.Observable
import scala.reflect.runtime.universe.TypeTag

/**
 * Contains `dialect` and `script` ports that define a textual script in one of the supported dialects.
 */
trait ScriptedBlock[U] { self: AbstractRxBlock[_] =>
  implicit val ttag: TypeTag[U]
  
  def dialect(lang: ScriptDialect): ScriptedBlock[U] with AbstractRxBlock[_] = this having (dialect <~ lang)
  def script(code: String): ScriptedBlock[U] with AbstractRxBlock[_] = this having (script <~ code)

  val dialect = Port[ScriptDialect]("dialect")
  val script = Port[String]("script")

  protected def scriptStream: Observable[Script[U]] = dialect.in combineLatest script.in map {
    case (lang, code) => new DefaultScript[U](lang, code)
  }
}