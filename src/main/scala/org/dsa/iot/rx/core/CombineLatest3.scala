package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxMerger3

/**
 * Combines three Observables into a single Observable of Tuple3, emitting a new tuple each time any
 * of the sources emits a new item.
 * 
 * @see <a href="http://reactivex.io/documentation/operators/combinelatest.html">ReactiveX operators documentation: CombineLatest</a>
 */
class CombineLatest3[T1, T2, T3] extends RxMerger3[T1, T2, T3, (T1, T2, T3)] {
  protected def compute = source1.in combineLatest source2.in combineLatest source3.in map {
    case ((i1, i2), i3) => (i1, i2, i3)
  }
}

/**
 * Factory for [[CombineLatest3]] instances.
 */
object CombineLatest3 {

  /**
   * Creates a new CombineLatest3 instance.
   */
  def apply[T1, T2, T3]: CombineLatest3[T1, T2, T3] = new CombineLatest3[T1, T2, T3]
}