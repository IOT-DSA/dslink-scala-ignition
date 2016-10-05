package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import org.dsa.iot.scala.Having

/**
 * Adds extra columns to the DataFrame.
 */
class AddFields(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def fields(values: (String, Any)*): AddFields = this having (fields <~ values)

  def add(tuple: (String, Any)): AddFields = this having (fields.add <~ tuple)
  def %(tuple: (String, Any)): AddFields = add(tuple)

  def add(name: String, value: Any): AddFields = add(name -> value)
  def %(name: String, value: Any): AddFields = add(name, value)

  val fields = PortList[(String, Any)]("fields")

  protected def compute = fields.combinedIns flatMap { list => doTransform(com.ignition.frame.AddFields(list)) }
}

/**
 * Factory for [[AddFields]] instances.
 */
object AddFields {

  /**
   * Creates a new AddFields instance.
   */
  def apply()(implicit rt: SparkRuntime): AddFields = new AddFields
}