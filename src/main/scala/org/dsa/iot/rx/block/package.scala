package org.dsa.iot.rx

import org.dsa.iot.dslink.node.value.Value

import rx.lang.scala.Observable

package object block {

  type ValueStream = Observable[Value]

  type DSARxBlock = AbstractRxBlock[Seq[Value], Seq[ValueStream], Value]
}