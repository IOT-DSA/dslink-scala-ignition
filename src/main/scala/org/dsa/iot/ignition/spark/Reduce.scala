package org.dsa.iot.ignition.spark

import com.ignition.frame.ReduceOp.ReduceOp
import com.ignition.frame.SparkRuntime

import org.dsa.iot.scala.Having

/**
 * Performs reduceByKey() function by grouping the rows by the selected key first, and then
 * applying a list of reduce functions to the specified data columns.
 */
class Reduce(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def columns(values: (String, ReduceOp)*): Reduce = this having (columns <~ values)

  def add(tuple: (String, ReduceOp)): Reduce = this having (columns.add <~ tuple)
  def %(tuple: (String, ReduceOp)): Reduce = add(tuple)

  def add(name: String, value: ReduceOp): Reduce = add(name -> value)
  def %(name: String, value: ReduceOp): Reduce = add(name, value)

  def groupBy(fields: String*): Reduce = this having (groupBy <~ fields.toList)

  val columns = PortList[(String, ReduceOp)]("columns")
  val groupBy = Port[List[String]]("groupBy")

  protected def compute = (groupBy.in combineLatest columns.combinedIns) flatMap {
    case (grp, flds) => doTransform(com.ignition.frame.Reduce(flds, grp))
  }
}

/**
 * Factory for [[Reduce]] instances.
 */
object Reduce {

  /**
   * Creates a new Reduce instance.
   */
  def apply()(implicit rt: SparkRuntime): Reduce = new Reduce groupBy ()
}