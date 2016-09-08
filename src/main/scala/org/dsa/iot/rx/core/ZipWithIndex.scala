package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Combines the items from the source Observable with their indices.
 */
class ZipWithIndex[T] extends RxTransformer[T, (T, Int)] {
  protected def compute = source.in zipWithIndex
}

/**
 * Factory for [[ZipWithIndex]] instances.
 */
object ZipWithIndex {
  
  /**
   * Creates a new ZipWithIndex instance.
   */
  def apply[T]: ZipWithIndex[T] = new ZipWithIndex[T]
}