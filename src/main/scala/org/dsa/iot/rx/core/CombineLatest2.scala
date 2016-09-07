package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxMerger2

/**
 * Combines two Observables into a single Observable of Tuple2, emitting a new tuple each time either
 * of the sources emits a new item.
 */
class CombineLatest2[T1, T2] extends RxMerger2[T1, T2, (T1, T2)] {
  protected def compute = source1.in combineLatest source2.in
}

/**
 * Factory for [[CombineLatest2]] instances.
 */
object CombineLatest2 {

  /**
   * Creates a new CombineLatest2 instance.
   */
  def apply[T1, T2]: CombineLatest2[T1, T2] = new CombineLatest2[T1, T2]
}