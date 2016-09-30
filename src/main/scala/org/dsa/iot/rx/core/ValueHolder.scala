package org.dsa.iot.rx.core

import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.Having

import rx.lang.scala.Observable

/**
 * Forwards the values passed into its `value` port.
 * 
 * <img width="640" height="315" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/from.png" alt="" />
 */
class ValueHolder[A] extends AbstractRxBlock[A] {
  
  def value(v: A): ValueHolder[A] = this having (value <~ v)
  def source(o: Observable[A]): ValueHolder[A] = this having (value <~ o)
  
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