package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Emits the very last item emitted by the source, or a default if the source is empty.
 *
 * <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/lastOrDefault.png" alt="" />
 */
class Last[T] extends RxTransformer[T, T] {

  def default(value: T): Last[T] = this having (default <~ Some(value))
  def noDefault(): Last[T] = this having (default <~ None)

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
  def apply[T]: Last[T] = create(None)

  /**
   * Creates a new Last instance with a default value.
   */
  def apply[T](default: T): Last[T] = create(Some(default))

  private def create[T](default: Option[T]) = {
    val block = new Last[T]
    block.default <~ default
    block
  }
}