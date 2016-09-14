package org.dsa.iot.ignition.core

import org.dsa.iot.dslink.link.Requester
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.DSAHelper

/**
 * Reads values from a DSA node.
 */
class DSAInput(implicit requester: Requester) extends AbstractRxBlock[Value] {
  val path = Port[String]("path")

  protected def compute = path.in flatMap (p => DSAHelper.watch(p).map(_.getValue))
}

/**
 * Factory for [[DSAInput]] instances.
 */
object DSAInput {

  /**
   * Creates a new DSAInput instance.
   */
  def apply()(implicit requester: Requester) = new DSAInput

  /**
   * Creates a new DSAInput instance for the specified path.
   */
  def apply(path: String)(implicit requester: Requester) = {
    val block = new DSAInput
    block.path <~ path
    block
  }
}