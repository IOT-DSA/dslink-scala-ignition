package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Tests whether the source emits no elements.
 */
class IsEmpty extends RxTransformer[Any, Boolean] {
  protected def compute = source.in.isEmpty
}

/**
 * Factory for [[IsEmpty]] instances.
 */
object IsEmpty {
  
  /**
   * Creates a new IsEmpty instance.
   */
  def apply(): IsEmpty = new IsEmpty
}