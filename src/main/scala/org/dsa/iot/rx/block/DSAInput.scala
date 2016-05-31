package org.dsa.iot.rx.block

import org.dsa.iot.DSAHelper
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.rx.RxMain.requester
import org.dsa.iot.valueToString

case class DSAInput() extends DSARxBlock {
  val path = Port[Value]
  private var lastPath: Option[String] = None

  protected def combineAttributes = path.in map (Seq(_))

  protected def combineInputs = Nil

  protected def evaluator(attrs: Seq[Value]) = {
    val path = attrs.head: String

    lastPath foreach DSAHelper.unwatch
    lastPath = Some(path)

    (_: Seq[ValueStream]) => DSAHelper.watch(path).map(_.getValue)
  }
}