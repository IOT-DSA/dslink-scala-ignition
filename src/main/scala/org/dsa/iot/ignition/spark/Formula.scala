package org.dsa.iot.ignition.spark

import org.apache.spark.sql.types.DataType

import com.ignition.frame.SparkRuntime
import com.ignition.script.RowExpression
import org.dsa.iot.scala.Having

import rx.lang.scala.Observable

/**
 * Calculates new fields based on string expressions in various dialects.
 */
class Formula(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def add(tuple: (String, RowExpression[_ <: DataType])): Formula = this having (fields.add <~ tuple)
  def %(tuple: (String, RowExpression[_ <: DataType])): Formula = add(tuple)

  def add(name: String, expr: RowExpression[_ <: DataType]): Formula = add(name -> expr)
  def %(name: String, expr: RowExpression[_ <: DataType]): Formula = add(name -> expr)

  def fields(tuples: (String, RowExpression[_ <: DataType])*): Formula = this having (fields <~ tuples)

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