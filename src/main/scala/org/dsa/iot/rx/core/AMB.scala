package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxMergerN

import rx.lang.scala.Observable

/**
 * Mirrors the one Observable in an Iterable of several Observables that first emits an item.
 * 
 * <img width="640" height="385" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/amb.png" alt="" />
 */
class AMB[T] extends RxMergerN[T, T] {
  protected def compute = Observable.amb(sources.ins: _*)
}

/**
 * Factory for [[AMB]] instances.
 */
object AMB {

  /**
   * Creates a new AMB instance.
   */
  def apply[T]: AMB[T] = new AMB[T]
}