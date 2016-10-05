package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.Having

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Reads a folder of text files.
 */
class TextFolderInput(implicit rt: SparkRuntime) extends AbstractRxBlock[DataFrame] {

  def path(str: String): TextFolderInput = this having (path <~ str)
  def nameField(field: String): TextFolderInput = this having (nameField <~ field)
  def dataField(field: String): TextFolderInput = this having (dataField <~ field)

  val path = Port[String]("path")
  val nameField = Port[String]("nameField")
  val dataField = Port[String]("dataField")

  protected def compute = path.in combineLatest nameField.in combineLatest dataField.in flatMap {
    case ((path, name), data) =>
      val tfi = com.ignition.frame.TextFolderInput(path, name, data)
      Observable.just(tfi.output)
  }
}

/**
 * Factory for [[TextFolderInput]] instances.
 */
object TextFolderInput {

  /**
   * Creates a new TextFolderInput instance with name field as "filename"
   * and data field as "content".
   */
  def apply()(implicit rt: SparkRuntime): TextFolderInput = {
    new TextFolderInput nameField "filename" dataField "content"
  }
}