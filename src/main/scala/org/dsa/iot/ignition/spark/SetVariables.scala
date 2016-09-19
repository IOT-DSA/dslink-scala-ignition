package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime

/**
 * Sets or drops the ignition runtime variables.
 */
class SetVariables(implicit rt: SparkRuntime) extends RxFrameTransformer {
  val vars = PortList[(String, Any)]("vars")

  protected def compute = vars.combinedIns flatMap { list =>
    doTransform(com.ignition.frame.SetVariables(list.toMap))
  }
}

/**
 * Factory for [[SetVariables]] instances.
 */
object SetVariables {

  /**
   * Creates a new SetVariables instance.
   */
  def apply()(implicit rt: SparkRuntime): SetVariables = new SetVariables
}