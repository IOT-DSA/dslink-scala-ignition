package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Retries the sequence emitted by the source in case of an error, either indefinitely
 * or at most `count` times.
 * 
 * <img width="640" height="315" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/retry.png" alt="" />
 */
class Retry[T] extends RxTransformer[T, T] {
  val count = Port[Option[Long]]("count")

  protected def compute = count.in flatMap {
    case None    => source.in.retry
    case Some(n) => source.in.retry(n)
  }
}

/**
 * Factory for [[Retry]] instances.
 */
object Retry {

  /**
   * Creates a new Retry instance that retries indefinitely.
   */
  def apply[T]: Retry[T] = create(None)

  /**
   * Creates a new Retry instance that retries at most `count` times.
   */
  def apply[T](count: Int): Retry[T] = create(Some(count))

  private def create[T](count: Option[Long]) = {
    val block = new Retry[T]
    block.count <~ count
    block
  }
}