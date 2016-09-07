package org.dsa.iot.rx.core

import org.dsa.iot.rx.AbstractRxBlock

import rx.lang.scala.Observable

/**
 * Combines items passed into its ports into a single Observable.
 */
class FromList[A] extends AbstractRxBlock[A] {
  val items = PortList[A]("items")

  protected def compute = Observable.combineLatest(items.ins.toIterable)(identity) flatMap Observable.from[A]
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