package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxMergerN

import rx.lang.scala.Observable

/**
 * Combines multiple Observables into a single Observable of lists. Unlike [[CombineLatest]] it emits
 * a new value only when ''all'' sources have emitted the next item.
 */
class Zip[T] extends RxMergerN[T, Seq[T]] {
  protected def compute = Observable.zip(Observable.from(sources.ins))
}

/**
 * Factory for [[Zip]] instances.
 */
object Zip {
  
  /**
   * Creates a new Zip instance.
   */
  def apply[T]: Zip[T] = new Zip[T]
}