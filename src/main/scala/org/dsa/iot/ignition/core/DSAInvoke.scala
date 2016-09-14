package org.dsa.iot.ignition.core

import org.dsa.iot.dslink.link.Requester
import org.dsa.iot.dslink.node.actions.table.Table
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.{ DSAHelper, valueToList }

/**
 * Invokes a DSA action.
 */
class DSAInvoke(implicit requester: Requester) extends RxTransformer[Value, Table] {

  protected def compute = source.in flatMap { v =>
    val list = valueToList(v)
    val path = list(0).toString
    val params = list(1).asInstanceOf[Map[String, Any]]
    DSAHelper.invoke(path, params) map (_.getTable)
  }
}

/**
 * Factory for [[DSAInvoke]] instances.
 */
object DSAInvoke {

  /**
   * Creates a new DSAInvoke instance.
   */
  def apply()(implicit requester: Requester) = new DSAInvoke
}