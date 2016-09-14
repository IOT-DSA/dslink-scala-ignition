package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxMerger3

/**
 * Combines three Observables into a single Observable of Tuple3, emitting a new tuple
 * after all sources emitted the next item.
 */
class Zip3[T1, T2, T3] extends RxMerger3[T1, T2, T3, (T1, T2, T3)] {
  protected def compute = source1.in zip source2.in zip source3.in map {
    case ((i1, i2), i3) => (i1, i2, i3)
  }
}

/**
 * Factory for [[Zip3]] instances.
 */
object Zip3 {

  /**
   * Creates a new Zip3 instance.
   */
  def apply[T1, T2, T3]: Zip3[T1, T2, T3] = new Zip3[T1, T2, T3]
}