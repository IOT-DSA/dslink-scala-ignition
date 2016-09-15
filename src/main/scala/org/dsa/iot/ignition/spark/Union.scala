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
    val isn = com.ignition.frame.Union()
    val inputs = Observable.combineLatest(sources.ins.toIterable)(identity)
    inputs map { dfs =>
      dfs.zipWithIndex foreach { case (df, idx) => producer(df) --> isn.in(idx) }
      isn.output
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