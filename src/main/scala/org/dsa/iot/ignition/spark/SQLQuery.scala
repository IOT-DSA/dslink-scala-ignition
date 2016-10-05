package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.RxMergerN
import org.dsa.iot.scala.Having

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Executes an SQL statement against the inputs. Each input is injected as a table
 * under the name "inputX" where X is the index of the input.
 */
class SQLQuery(implicit rt: SparkRuntime) extends RxMergerN[DataFrame, DataFrame] {
  
  def query(sql: String): SQLQuery = this having (query <~ sql)
  
  val query = Port[String]("query")

  protected def compute = query.in flatMap { cql =>
    val sqlq = com.ignition.frame.SQLQuery(cql)
    sources.combinedIns map { dfs =>
      dfs.zipWithIndex foreach {
        case (df, idx) => producer(df) --> sqlq.in(idx)
      }
      sqlq.output
    }
  }
}

/**
 * Factory for [[SQLQuery]] instances.
 */
object SQLQuery {

  /**
   * Creates a new SQLQuery instance.
   */
  def apply()(implicit rt: SparkRuntime): SQLQuery = new SQLQuery
}