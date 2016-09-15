package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime

/**
 * Writes rows into a Cassandra table.
 */
class CassandraOutput(implicit rt: SparkRuntime) extends RxFrameTransformer {
  val keyspace = Port[String]("keyspace")
  val table = Port[String]("table")

  protected def compute = (keyspace.in combineLatest table.in) flatMap {
    case (ks, tbl) => doTransform(com.ignition.frame.CassandraOutput(ks, tbl))
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