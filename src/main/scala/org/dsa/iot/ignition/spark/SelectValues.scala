package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import com.ignition.frame.SelectAction

/**
 * Modifies, deletes, retains columns in the data rows.
 */
class SelectValues(implicit rt: SparkRuntime) extends RxFrameTransformer {
  val actions = PortList[SelectAction]("actions")

  protected def compute = actions.combinedIns flatMap {
    actions => doTransform(com.ignition.frame.SelectValues(actions))
  }
}

/**
 * Factory for [[SelectValues]] instance.
 */
object SelectValues {
  
  /**
   * Creates a new SelectValues instance.
   */
  def apply()(implicit rt: SparkRuntime): SelectValues = new SelectValues
}