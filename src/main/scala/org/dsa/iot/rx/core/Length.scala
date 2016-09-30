package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 * A shortcut for [[Count]] with a trivial predicate that always returns `true`, i.e. it
 * counts all the items in the source sequence.
 */
class Length extends Count[Any] {
  this.predicate <~ (_ => true)
}

/**
 * Factory for [[Length]] instances.
 */
object Length {

  /**
   * Creates a new Length instance for either outputting running totals for each item,
   * or just the final value.
   */
  def apply(rolling: Boolean = true): Length = {
    val block = new Length
    block.rolling <~ rolling
    block
  }
}