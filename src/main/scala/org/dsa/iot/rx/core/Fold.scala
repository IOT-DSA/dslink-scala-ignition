package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Repeatedly applies a function, where the first argument is the result obtained in the previous
 * application (first time - the `initial` value), and the second argument is each element of the
 * source sequence. Unlike `scan` it only emits the final result when the source sequence is complete.
 */
class Fold[T, R] extends RxTransformer[T, R] {
  val initial = Port[R]("initial")
  val accumulator = Port[(R, T) => R]("accumulator")

  protected def compute = (initial.in combineLatest accumulator.in) flatMap {
    case (init, acc) => source.in.foldLeft(init)(acc)
  }
}

/**
 * Factory for [[Fold]] instances.
 */
object Fold {

  /**
   * Creates a new Fold instance.
   */
  def apply[T, R]: Fold[T, R] = new Fold[T, R]

  /**
   * Creates a new Fold instance for the given initial value and accumulator function.
   */
  def apply[T, R](initial: R, accumulator: (R, T) => R): Fold[T, R] = {
    val block = new Fold[T, R]
    block.initial <~ initial
    block.accumulator <~ accumulator
    block
  }
}