package org.dsa.iot.ignition.spark

import com.ignition.frame.ReduceOp.ReduceOp
import com.ignition.frame.SparkRuntime

/**
 * Performs reduceByKey() function by grouping the rows by the selected key first, and then
 * applying a list of reduce functions to the specified data columns.
 */
class Reduce(implicit rt: SparkRuntime) extends RxFrameTransformer {
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
   * Creates a new Reduce instance with the specified GROUP BY columns.
   */
  def apply(groupBy: List[String] = Nil)(implicit rt: SparkRuntime): Reduce = {
    val block = new Reduce
    block.groupBy <~ groupBy
    block
  }
}