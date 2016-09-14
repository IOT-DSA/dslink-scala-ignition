package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Emits the very last item emitted by the source, or a default if the source is empty.
 * 
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/lastOrDefault.png" alt="" />
 */
class Last[T] extends RxTransformer[T, T] {
  val default = Port[Option[T]]("default")

  protected def compute = default.in flatMap {
    case Some(x) => source.in.lastOrElse(x)
    case None    => source.in.last
  }
}

/**
 * Factory for [[Last]] instances.
 */
object Last {
  
  /**
   * Creates a new Last instance without a default value.
   */
  def apply[T]: Last[T] = {
    val block = new Last[T]
    block.default <~ None
    block
  }

  /**
   * Creates a new Last instance with a default value.
   */
  def apply[T](default: T): Last[T] = {
    val block = new Last[T]
    block.default <~ Some(default)
    block
  }
}