package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.AbstractRxBlock

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Reads the text file into a data frame with a single column.
 * If the separator is specified, splits the file into multiple rows, otherwise
 * the data frame will contain only one row.
 */
class TextFileInput(implicit rt: SparkRuntime) extends AbstractRxBlock[DataFrame] {
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
   * Creates a new TextFileInput instance with the specified separator.
   */
  def apply(separator: Option[String] = None)(implicit rt: SparkRuntime): TextFileInput = {
    val block = new TextFileInput
    block.separator <~ separator
    block
  }
}