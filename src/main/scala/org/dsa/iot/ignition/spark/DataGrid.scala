package org.dsa.iot.ignition.spark

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.{ StructField, StructType }
import org.dsa.iot.rx.AbstractRxBlock

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Creates data frames from static data grid.
 */
class DataGrid(implicit rt: SparkRuntime) extends AbstractRxBlock[DataFrame] {
  val columns = PortList[StructField]("columns")
  val rows = Port[List[Row]]("rows")

  protected def compute = columns.combinedIns combineLatest rows.in flatMap {
    case (cols, rows) =>
      val schema = StructType(cols)
      val dg = com.ignition.frame.DataGrid(schema, rows)
      Observable.just(dg.output)
  }
}

/**
 * Factory for [[DataGrid]] instances.
 */
object DataGrid {

  /**
   * Creates a new DataGrid instance.
   */
  def apply()(implicit rt: SparkRuntime): DataGrid = new DataGrid
}