package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ StructField, StructType }
import com.ignition.frame.SparkRuntime
import com.ignition.rx.AbstractRxBlock
import rx.lang.scala.Observable

/**
 * Reads a CSV file and generates a spark data frame.
 */
class CsvFileInput(implicit rt: SparkRuntime) extends AbstractRxBlock[DataFrame] {
  val path = Port[String]("path")
  val separator = Port[Option[String]]("separator")
  val columns = PortList[StructField]("columns")

  protected def compute = {
    val fields = Observable.combineLatest(columns.ins.toIterable)(identity)
    path.in combineLatest separator.in combineLatest fields flatMap {
      case ((p, sep), cols) =>
        val schema = if (cols.isEmpty) None else Some(StructType(cols))
        val cfi = com.ignition.frame.CsvFileInput(p, sep, schema)
        Observable.just(cfi.output)
    }
  }
}