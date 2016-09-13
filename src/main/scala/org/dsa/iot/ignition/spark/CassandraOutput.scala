package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.RxTransformer

import com.ignition.frame.SparkRuntime

/**
 * Writes rows into a Cassandra table.
 */
class CassandraOutput(implicit rt: SparkRuntime) extends RxTransformer[DataFrame, DataFrame] {
  val keyspace = Port[String]("keyspace")
  val table = Port[String]("table")

  protected def compute = (keyspace.in combineLatest table.in) flatMap {
    case (ks, tbl) =>
      val co = com.ignition.frame.CassandraOutput(ks, tbl)
      source.in map { df =>
        val source = producer(df)
        source --> co
        co.output
      }
  }
}

/**
 * Factory for [[CassandraOutput]] instances.
 */
object CassandraOutput {

  /**
   * Creates a new CassandraOutput instance.
   */
  def apply()(implicit rt: SparkRuntime): CassandraOutput = new CassandraOutput
}