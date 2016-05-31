package org.dsa.iot.rx.block

import org.dsa.iot.{ listToValue, valueToAny }
import org.dsa.iot.rx._
import org.dsa.iot.dslink.node.value.Value
import rx.lang.scala.Observable

import com.ignition._

case class CsvFileInput() extends DSARxBlock {
  import RxMain.sparkRuntime
  
  val path = Port[Value]
  val separator = Port[Value]
  val columns = PortList[Value]

  protected def combineAttributes = {
    val sources = path.in :: separator.in :: (columns.ports map (_.in))
    Observable.combineLatest(sources)(identity)
  }
  protected def combineInputs: Seq[org.dsa.iot.rx.block.ValueStream] = Nil
  
  protected def evaluator(attrs: Seq[Value]) = attrs.toList match {
    case path :: separator :: columns =>
      val cfi = frame.CsvFileInput(path.getString, separator.getString)
      (_: Seq[ValueStream]) => Observable.just(dataFrameToTableData(cfi.output(0)))
    case _ => (_: Seq[ValueStream]) => Observable.never
  }
}