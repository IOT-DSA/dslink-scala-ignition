package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import org.dsa.iot.scala.Having

/**
 * Calculates column-based statistics using MLLib library.
 */
class ColumnStats(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def columns(cols: String*): ColumnStats = this having (columns <~ cols)
  def groupBy(fields: String*): ColumnStats = this having (groupBy <~ fields.toList)

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
   * Creates a new ColumnStats instance.
   */
  def apply()(implicit rt: SparkRuntime): ColumnStats = new ColumnStats groupBy ()
}