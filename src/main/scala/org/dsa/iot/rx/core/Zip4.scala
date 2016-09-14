package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxMerger4

/**
 * Combines three Observables into a single Observable of Tuple4, emitting a new tuple
 * after all sources emitted the next item.
 */
class Zip4[T1, T2, T3, T4] extends RxMerger4[T1, T2, T3, T4, (T1, T2, T3, T4)] {
  protected def compute = source1.in zip source2.in zip source3.in zip source4.in map {
    case (((i1, i2), i3), i4) => (i1, i2, i3, i4)
  }
}

/**
 * Factory for [[Zip4]] instances.
 */
object Zip4 {

  /**
   * Creates a new Zip4 instance.
   */
  def apply[T1, T2, T3, T4]: Zip4[T1, T2, T3, T4] = new Zip4[T1, T2, T3, T4]
}