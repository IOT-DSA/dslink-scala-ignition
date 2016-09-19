package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime

/**
 * Calculates column-based statistics using MLLib library.
 */
class ColumnStats(implicit rt: SparkRuntime) extends RxFrameTransformer {
  val columns = PortList[String]("columns")
  val groupBy = Port[List[String]]("groupBy")

  protected def compute = columns.combinedIns combineLatest groupBy.in flatMap {
    case (cols, grp) => doTransform(com.ignition.frame.mllib.ColumnStats(cols, grp))
  }
}

/**
 * Factory for [[ColumnStats]] instances.
 */
object ColumnStats {

  /**
   * Creates a new ColumnStats instance with the specified GROUP BY columns.
   */
  def apply(groupBy: List[String] = Nil)(implicit rt: SparkRuntime): ColumnStats = {
    val block = new ColumnStats
    block.groupBy <~ groupBy
    block
  }
}