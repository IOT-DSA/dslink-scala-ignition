package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.Having

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Reads the text file into a data frame with a single column.
 * If the separator is specified, splits the file into multiple rows, otherwise
 * the data frame will contain only one row.
 */
class TextFileInput(implicit rt: SparkRuntime) extends AbstractRxBlock[DataFrame] {

  def path(str: String): TextFileInput = this having (path <~ str)
  def separator(str: String): TextFileInput = this having (separator <~ Some(str))
  def noSeparator(): TextFileInput = this having (separator <~ None)
  def field(str: String): TextFileInput = this having (field <~ str)

  val path = Port[String]("path")
  val separator = Port[Option[String]]("separator")
  val field = Port[String]("field")

  protected def compute = path.in combineLatest separator.in combineLatest field.in flatMap {
    case ((path, sep), fld) =>
      val tfi = com.ignition.frame.TextFileInput(path, sep, fld)
      Observable.just(tfi.output)
  }
}

/**
 * Factory for [[TextFileInput]] instances.
 */
object TextFileInput {

  /**
   * Creates a new TextFileInput instance with no separator.
   */
  def apply()(implicit rt: SparkRuntime): TextFileInput = new TextFileInput noSeparator ()
}