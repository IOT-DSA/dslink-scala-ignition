package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxMerger2

/**
 * Flattens two Observables into one Observable, without any transformation.
 */
class Merge[T] extends RxMerger2[T, T, T] {
  protected def compute = source1.in merge source2.in
}

/**
 * Factory for [[Merge]] instances.
 */
object Merge {

  /**
   * Creates a new Merge instance.
   */
  def apply[T]: Merge[T] = new Merge[T]
}