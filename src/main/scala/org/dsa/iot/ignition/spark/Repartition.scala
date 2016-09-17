package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime

/**
 * Repartitions the underlying Spark RDD.
 */
class Repartition(implicit rt: SparkRuntime) extends RxFrameTransformer {
  val size = Port[Int]("size")
  val shuffle = Port[Boolean]("shuffle")

  protected def compute = size.in combineLatest shuffle.in flatMap {
    case (size, shuffle) => doTransform(com.ignition.frame.Repartition(size, shuffle))
  }
}

/**
 * Factory for [[Repartition]] instances.
 */
object Repartition {

  /**
   * Creates a new Repartition instance.
   */
  def apply()(implicit rt: SparkRuntime): Repartition = new Repartition
}