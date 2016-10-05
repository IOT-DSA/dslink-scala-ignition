package org.dsa.iot.ignition.spark

import com.ignition.frame.BasicAggregator.BasicAggregator
import com.ignition.frame.SparkRuntime

import org.dsa.iot.scala.Having

/**
 * Calculates basic statistics.
 */
class BasicStats(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def add(tuple: (String, BasicAggregator)): BasicStats = this having (columns.add <~ tuple)
  def %(tuple: (String, BasicAggregator)): BasicStats = add(tuple)

  def add(field: String, functions: BasicAggregator*): BasicStats = {
    functions foreach (f => add(field -> f))
    this
  }
  def %(field: String, functions: BasicAggregator*): BasicStats = add(field, functions: _*)

  def groupBy(fields: String*): BasicStats = this having (groupBy <~ fields.toList)

  val columns = PortList[(String, BasicAggregator)]("columns")
  val groupBy = Port[List[String]]("groupBy")

  protected def compute = (groupBy.in combineLatest columns.combinedIns) flatMap {
    case (grp, flds) => doTransform(com.ignition.frame.BasicStats(flds, grp))
  }
}

/**
 * Factory for [[BasicStats]] instances.
 */
object BasicStats {

  /**
   * Creates a new BasicStats instance.
   */
  def apply()(implicit rt: SparkRuntime): BasicStats = new BasicStats groupBy ()
}