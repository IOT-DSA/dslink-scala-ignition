package org.dsa.iot.rx.core

import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.Having

import rx.lang.scala.Observable

/**
 * Combines items passed into its ports into a single Observable.
 * 
 * <img width="640" height="315" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/from.png" alt="" />
 */
class FromList[A] extends AbstractRxBlock[A] {
  
  def items(values: A*): FromList[A] = this having (items <~ values)
  def add(value: A): FromList[A] = this having (items.add <~ value)
  
  val items = PortList[A]("items")

  protected def compute = items.combinedIns flatMap Observable.from[A]
}

/**
 * Factory for [[FromList]] instances.
 */
object FromList {

  /**
   * Creates a new Sequence instance.
   */
  def apply[A]: FromList[A] = new FromList[A]

  /**
   * Creates a new Sequence from the list of values.
   */
  def apply[A](values: A*): FromList[A] = {
    val block = new FromList[A]
    block.items <~ (values: _*)
    block
  }
}