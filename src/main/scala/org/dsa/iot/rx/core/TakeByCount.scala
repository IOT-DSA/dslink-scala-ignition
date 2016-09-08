package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Emits only first `count` items emitted by the source.
 */
class TakeByCount[T] extends RxTransformer[T, T] {
  val count = Port[Int]("count")

  protected def compute = count.in flatMap source.in.take
}

/**
 * Factory for [[TakeByCount]] instances.
 */
object TakeByCount {

  /**
   * Creates a new TakeByCount instance.
   */
  def apply[T]: TakeByCount[T] = new TakeByCount[T]

  /**
   * Creates a new TakeByCount instance for the given count.
   */
  def apply[T](count: Int): TakeByCount[T] = {
    val block = new TakeByCount[T]
    block.count <~ count
    block
  }
}