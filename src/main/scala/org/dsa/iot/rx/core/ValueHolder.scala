package org.dsa.iot.rx.core

import org.dsa.iot.rx.AbstractRxBlock

import rx.lang.scala.Observable

/**
 * Forwards the values passed into its `value` port.
 * 
 * <img width="640" height="315" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/from.png" alt="" />
 */
class ValueHolder[A] extends AbstractRxBlock[A] {
  val value = Port[A]("value")

  protected def compute = value.in flatMap (Observable.just(_))
}

/**
 * Factory for [[ValueHolder]] instances.
 */
object ValueHolder {

  /**
   * Creates a new ValueHolder instance.
   */
  def apply[A]: ValueHolder[A] = new ValueHolder[A]
  
  /**
   * Creates a new ValueHolder instance and initializes its port to the specified value.
   */
  def apply[A](value: A): ValueHolder[A] = {
    val block = new ValueHolder[A]
    block.value <~ value
    block
  }

  /**
   * Creates a new ValueHolder instance and binds its port to the specified Observable.
   */
  def apply[A](source: Observable[A]): ValueHolder[A] = {
    val block = new ValueHolder[A]
    block.value <~ source
    block
  }
}