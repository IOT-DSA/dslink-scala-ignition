package org.dsa.iot.ignition.spark

import org.apache.spark.sql.types.DataType

import com.ignition.frame.SparkRuntime
import com.ignition.script.RowExpression

import rx.lang.scala.Observable

/**
 * Calculates new fields based on string expressions in various dialects.
 */
class Formula(implicit rt: SparkRuntime) extends RxFrameTransformer {
  val fields = PortList[(String, RowExpression[_ <: DataType])]("fields")

  protected def compute = fields.combinedIns flatMap { list => doTransform(com.ignition.frame.Formula(list)) }
}

/**
 * Factory for [[Formula]] instances.
 */
object Formula {

  /**
   * Creates a new Formula instance.
   */
  def apply()(implicit rt: SparkRuntime): Formula = new Formula
}