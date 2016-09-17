package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime

/**
 * Caches each DataFrame that arrives to this block.
 */
class Cache(implicit rt: SparkRuntime) extends RxFrameTransformer {
  protected def compute = doTransform(com.ignition.frame.Cache())
}

/**
 * Factory for [[Cache]] instances.
 */
object Cache {

  /**
   * Creates a new Cache instance.
   */
  def apply()(implicit rt: SparkRuntime): Cache = new Cache
}