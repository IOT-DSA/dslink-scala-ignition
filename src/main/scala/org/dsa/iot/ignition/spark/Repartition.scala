package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import org.dsa.iot.scala.Having

/**
 * Repartitions the underlying Spark RDD.
 */
class Repartition(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def size(n: Int): Repartition = this having (size <~ n)
  def shuffle(flag: Boolean): Repartition = this having (shuffle <~ flag)

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
   * Creates a new Repartition instance with no shuffle.
   */
  def apply()(implicit rt: SparkRuntime): Repartition = new Repartition shuffle false
}