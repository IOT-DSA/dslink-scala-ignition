package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Adds extra columns to the DataFrame.
 */
class AddFields(implicit rt: SparkRuntime) extends RxFrameTransformer {
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