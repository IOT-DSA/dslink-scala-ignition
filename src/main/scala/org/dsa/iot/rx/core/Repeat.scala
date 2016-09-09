package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Repeats the sequence of items emitted by the source either indefinitely or at most `count` times.
 */
class Repeat[T] extends RxTransformer[T, T] {
  val count = Port[Option[Long]]("count")

  protected def compute = count.in flatMap {
    case None    => source.in repeat
    case Some(n) => source.in repeat n
  }
}

/**
 * Factory for [[Repeat]] instances.
 */
object Repeat {

  /**
   * Creates a new Repeat instance that repeats source items indefinitely.
   */
  def apply[T]: Repeat[T] = create(None)

  /**
   * Creates a new Repeat instance that repeats source items at most `count` times.
   */
  def apply[T](count: Int): Repeat[T] = create(Some(count))

  private def create[T](count: Option[Long]) = {
    val block = new Repeat[T]
    block.count <~ count
    block
  }
}