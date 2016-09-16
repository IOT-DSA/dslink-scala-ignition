package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.RxMergerN

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Merges multiple DataFrames. All of them must have identical schema.
 */
class Union(implicit rt: SparkRuntime) extends RxMergerN[DataFrame, DataFrame] {

  protected def compute = {
    val union = com.ignition.frame.Union()
    sources.combinedIns map { dfs =>
      dfs.zipWithIndex foreach { case (df, idx) => producer(df) --> union.in(idx) }
      union.output
    }
  }
}

/**
 * Factory for [[Union]] instances.
 */
object Union {

  /**
   * Creates a new Union instance.
   */
  def apply()(implicit rt: SparkRuntime): Union = new Union
}