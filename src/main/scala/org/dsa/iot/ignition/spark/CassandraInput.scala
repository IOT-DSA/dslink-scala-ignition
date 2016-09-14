package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.AbstractRxBlock

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Reads rows from an Apache Cassandra table.
 */
class CassandraInput(implicit rt: SparkRuntime) extends AbstractRxBlock[DataFrame] {
  val keyspace = Port[String]("keyspace")
  val table = Port[String]("table")
  val columns = Port[List[String]]("columns")
  val where = Port[Option[String]]("where")

  protected def compute =
    (keyspace.in combineLatest table.in combineLatest columns.in combineLatest where.in) flatMap {
      case (((ks, tbl), cols), cql) =>
        val wh = cql map (com.ignition.frame.Where(_))
        val ci = com.ignition.frame.CassandraInput(ks, tbl, cols, wh)
        Observable.just(ci.output)
    }
}

/**
 * Factory for [[CassandraInput]] instances.
 */
object CassandraInput {

  /**
   * Creates a new CassandraInput instance with the specified columns and WHERE clause.
   */
  def apply(columns: List[String] = Nil,
            where: Option[String] = None)(implicit rt: SparkRuntime): CassandraInput = {
    val block = new CassandraInput
    block.columns <~ columns
    block.where <~ where
    block
  }
}