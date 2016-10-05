package org.dsa.iot.ignition.spark

import org.dsa.iot.scala.Having

import com.ignition.frame.SelectAction
import com.ignition.frame.SelectAction.{ Remove, Rename, Retain, Retype }
import com.ignition.frame.SparkRuntime

/**
 * Modifies, deletes, retains columns in the data rows.
 */
class SelectValues(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def actions(items: SelectAction*): SelectValues = this having (actions <~ items)

  def retain(names: String*): SelectValues = this having (actions.add <~ Retain(names.toSeq))
  def rename(pairs: (String, String)*): SelectValues = this having (actions.add <~ Rename(pairs.toMap))
  def remove(names: String*) = this having (actions.add <~ Remove(names.toSeq))
  def retype(pairs: (String, String)*) = this having (actions.add <~ Retype(pairs: _*))

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