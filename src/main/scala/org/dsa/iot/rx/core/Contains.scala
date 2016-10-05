package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Emits a Boolean that indicates whether the source emitted a specified item.
 * 
 * <img width="640" height="320" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/contains.png" alt="" />
 */
class Contains[T] extends RxTransformer[T, Boolean] {
  
  def item(value: T): Contains[T] = this having (item <~ value)
  
  val item = Port[T]("item")

  protected def compute = item.in flatMap source.in.contains
}

/**
 * Factory for [[Contains]] instances.
 */
object Contains {

  /**
   * Creates a new Contains instance.
   */
  def apply[T]: Contains[T] = new Contains[T]

  /**
   * Creates a new Contains instance for the specified item.
   */
  def apply[T](item: T): Contains[T] = {
    val block = new Contains[T]
    block.item <~ item
    block
  }
}