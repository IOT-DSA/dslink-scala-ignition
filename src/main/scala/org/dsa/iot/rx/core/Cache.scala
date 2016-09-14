package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Caches the items emitted by the source, so that future subscriptions can "replay" them from the
 * beginning.
 * 
 * <img width="640" height="410" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/cache.png" alt="" />
 */
class Cache[T] extends RxTransformer[T, T] {
  val capacity = Port[Option[Int]]("capacity")

  protected def compute = capacity.in flatMap {
    case Some(size) => source.in.cacheWithInitialCapacity(size)
    case _          => source.in.cache
  }
}

/**
 * Factory for [[Cache]] instances.
 */
object Cache {

  /**
   * Creates a new Cache instance with unlimited cache capacity.
   */
  def apply[T]: Cache[T] = create(None)

  /**
   * Creates a new Cache instance with the specified capacity.
   */
  def apply[T](capacity: Int): Cache[T] = create(Some(capacity))

  private def create[T](capacity: Option[Int]) = {
    val block = new Cache[T]
    block.capacity <~ capacity
    block
  }
}