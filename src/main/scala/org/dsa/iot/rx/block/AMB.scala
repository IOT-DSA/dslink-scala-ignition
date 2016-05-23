package org.dsa.iot.rx.block

import org.dsa.iot.dslink.node.value.Value

import rx.lang.scala.Observable

case class AMB() extends DSARxBlock {
  val inputs = PortList[Value]

  protected def combineAttributes = Observable.just(Nil)

  protected def combineInputs = inputs.ports map (_.in)

  protected def evaluator(attrs: Seq[Value]) = (inputs: Seq[ValueStream]) => Observable.amb(inputs: _*)
}