package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.RxMergerN

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Finds the intersection of the two DataRow RDDs. They must have idential
 * metadata.
 */
class Intersection(implicit rt: SparkRuntime) extends RxMergerN[DataFrame, DataFrame] {

  protected def compute = {
    val isn = com.ignition.frame.Intersection()
    val inputs = Observable.combineLatest(sources.ins.toIterable)(identity)
    inputs map { dfs =>
      dfs.zipWithIndex foreach { case (df, idx) => producer(df) --> isn.in(idx) }
      isn.output
    }
  }
}

/**
 * Factory for [[Intersection]] instances.
 */
object Intersection {

  /**
   * Creates a new Intersection instance.
   */
  def apply()(implicit rt: SparkRuntime): Intersection = new Intersection
}