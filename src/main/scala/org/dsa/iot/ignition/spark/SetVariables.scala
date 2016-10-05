package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import org.dsa.iot.scala.Having

/**
 * Sets or drops the ignition runtime variables.
 */
class SetVariables(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def vars(values: (String, Any)*): SetVariables = this having (vars <~ values)

  def add(tuple: (String, Any)): SetVariables = this having (vars.add <~ tuple)
  def %(tuple: (String, Any)): SetVariables = add(tuple)

  def add(name: String, value: Any): SetVariables = add(name -> value)
  def %(name: String, value: Any): SetVariables = add(name, value)

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