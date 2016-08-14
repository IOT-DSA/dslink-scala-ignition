package org.dsa.iot.ignition.core

import org.dsa.iot.DSAHelper
import org.dsa.iot.dslink.link.Requester
import org.dsa.iot.dslink.node.value.Value

import com.ignition.rx.AbstractRxBlock

/**
 * Reads values from a DSA node.
 */
class DSAInput(implicit requester: Requester) extends AbstractRxBlock[Value] {
  val path = Port[String]("path")

  protected def compute = path.in flatMap (p => DSAHelper.watch(p).map(_.getValue))
}