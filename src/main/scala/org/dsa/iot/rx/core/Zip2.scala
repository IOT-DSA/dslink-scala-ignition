package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxMerger2

/**
 * Combines two Observables into a single Observable of Tuple2, emitting a new tuple
 * after both sources emitted the next item.
 */
class Zip2[T1, T2] extends RxMerger2[T1, T2, (T1, T2)] {
  protected def compute = source1.in zip source2.in
}

/**
 * Factory for [[Zip2]] instances.
 */
object Zip2 {

  /**
   * Creates a new Zip2 instance.
   */
  def apply[T1, T2]: Zip2[T1, T2] = new Zip2[T1, T2]
}