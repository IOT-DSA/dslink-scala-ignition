package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.Having

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Reads a JSON file, which contains a separate JSON object in each line.
 */
class JsonFileInput(implicit rt: SparkRuntime) extends AbstractRxBlock[DataFrame] {
  
  def path(str: String): JsonFileInput = this having (path <~ str)
  def fields(props: (String, String)*): JsonFileInput = this having (fields <~ props)
  
  def add(tuple: (String, String)): JsonFileInput = this having (fields.add <~ tuple)
  def %(tuple: (String, String)): JsonFileInput = add(tuple)

  def add(name: String, value: String): JsonFileInput = add(name -> value)
  def %(name: String, value: String): JsonFileInput = add(name, value)
  
  val path = Port[String]("path")
  val fields = PortList[(String, String)]("fields")

  protected def compute = path.in combineLatest fields.combinedIns flatMap {
    case (path, fields) =>
      val jfi = com.ignition.frame.JsonFileInput(path, fields)
      Observable.just(jfi.output)
  }
}

/**
 * Factory for [[JsonFileInput]] instances.
 */
object JsonFileInput {

  /**
   * Creates a new JsonFileInput instance.
   */
  def apply()(implicit rt: SparkRuntime): JsonFileInput = new JsonFileInput
}