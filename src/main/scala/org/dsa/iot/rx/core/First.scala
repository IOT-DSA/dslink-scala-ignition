package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Emits the very first item emitted by the source, or a default if the source is empty.
 * 
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/firstOrDefault.png" alt="" />
 */
class First[T] extends RxTransformer[T, T] {
  val default = Port[Option[T]]("default")

  protected def compute = default.in flatMap {
    case Some(x) => source.in.firstOrElse(x)
    case None    => source.in.first
  }
}

/**
 * Factory for [[First]] instances.
 */
object First {
  
  /**
   * Creates a new First instance without a default value.
   */
  def apply[T]: First[T] = {
    val block = new First[T]
    block.default <~ None
    block
  }

  /**
   * Creates a new First instance with a default value.
   */
  def apply[T](default: T): First[T] = {
    val block = new First[T]
    block.default <~ Some(default)
    block
  }
}