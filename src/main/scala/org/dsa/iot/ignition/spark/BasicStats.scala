package org.dsa.iot.ignition.spark

import com.ignition.frame.BasicAggregator.BasicAggregator
import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Calculates basic statistics.
 */
class BasicStats(implicit rt: SparkRuntime) extends RxFrameTransformer {
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
   * Creates a new BasicStats instance with the specified GROUP BY columns.
   */
  def apply(groupBy: List[String] = Nil)(implicit rt: SparkRuntime): BasicStats = {
    val block = new BasicStats
    block.groupBy <~ groupBy
    block
  }
}