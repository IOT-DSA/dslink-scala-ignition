package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxMergerN

import rx.lang.scala.Observable

/**
 * Combines multiple Observables into a single Observable of lists, emitting a new list each time any
 * of the sources emits a new item.
 * 
 * @see <a href="http://reactivex.io/documentation/operators/combinelatest.html">ReactiveX operators documentation: CombineLatest</a>
 */
class CombineLatest[T] extends RxMergerN[T, Seq[T]] {
  protected def compute = sources.combinedIns
}

/**
 * Factory for [[CombineLatest]] instances.
 */
object CombineLatest {

  /**
   * Creates a new CombineLatest instance.
   */
  def apply[T]: CombineLatest[T] = new CombineLatest[T]
}