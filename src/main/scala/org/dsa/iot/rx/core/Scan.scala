package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * Continuouly applies a function, where the first argument is the result obtained in the previous
 * application (first time - the `initial` value), and the second argument is each element of the
 * source sequence. Unlike `fold` it emits each intermediate result.
 * 
 * <img width="640" height="320" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/scanSeed.png" alt="" />
 */
class Scan[T, R] extends RxTransformer[T, R] {
  val initial = Port[R]("initial")
  val accumulator = Port[(R, T) => R]("accumulator")

  protected def compute = (initial.in combineLatest accumulator.in) flatMap {
    case (init, acc) => source.in.scan(init)(acc)
  }
}

/**
 * Factory for [[Scan]] instances.
 */
object Scan {

  /**
   * Creates a new Scan instance.
   */
  def apply[T, R]: Scan[T, R] = new Scan[T, R]

  /**
   * Creates a new Scan instance for the given initial value and accumulator function.
   */
  def apply[T, R](initial: R, accumulator: (R, T) => R): Scan[T, R] = {
    val block = new Scan[T, R]
    block.initial <~ initial
    block.accumulator <~ accumulator
    block
  }
}