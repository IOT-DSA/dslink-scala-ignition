package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import org.dsa.iot.scala.Having

/**
 * Filters the data frame based on a condition against fields.
 */
class Filter(implicit rt: SparkRuntime) extends RxFrameTransformer {
  
  def condition(str: String): Filter = this having (condition <~ str)
  
  val condition = Port[String]("condition")

  protected def compute = condition.in flatMap { sql => doTransform(com.ignition.frame.Filter(sql)) }
}

/**
 * Factory for [[Filter]] instances.
 */
object Filter {
  
  /**
   * Creates a new Filter instance.
   */
  def apply()(implicit rt: SparkRuntime): Filter = new Filter
}