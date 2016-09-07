package org.dsa.iot.rx.core

import org.dsa.iot.rx.AbstractRxBlock

import rx.lang.scala.Observable

/**
 * Emits items from the supplied collection.
 */
class Sequence[A] extends AbstractRxBlock[A] {
  val items = Port[Iterable[A]]("items")

  protected def compute = items.in flatMap Observable.from[A]
}

/**
 * Factory for [[Sequence]] instances.
 */
object Sequence {

  /**
   * Creates a new Sequence instance.
   */
  def apply[A]: Sequence[A] = new Sequence[A]

  /**
   * Creates a new Sequence instance from the supplied values.
   */
  def apply[A](values: A*): Sequence[A] = apply(values)

  /**
   * Creates a new Sequence instance from the supplied collection.
   */
  def apply[A](values: Iterable[A]): Sequence[A] = {
    val block = new Sequence[A]
    block.items <~ values
    block
  }
}