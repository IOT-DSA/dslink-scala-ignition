package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer

/**
 *  Drops a specified number of items from the beginning or end of source sequence.
 */
class DropByCount[T](right: Boolean) extends RxTransformer[T, T] {
  val count = Port[Int]("count")

  protected def compute = right match {
    case true  => count.in flatMap source.in.dropRight
    case false => count.in flatMap source.in.drop
  }
}

/**
 * Factory for [[DropByCount]] instances.
 */
object DropByCount {

  /**
   * Creates a new DropByCount instance. If `right` is true, drops items from the end of the sequence,
   * otherwise drops items from the beginning.
   */
  def apply[T](right: Boolean): DropByCount[T] = new DropByCount[T](right)

  /**
   * Creates a new DropByCount instance for the given item count. If `right` is true, drops items 
   * from the end of the sequence, otherwise drops items from the beginning.
   */
  def apply[T](count: Int, right: Boolean): DropByCount[T] = {
    val block = new DropByCount[T](right)
    block.count <~ count
    block
  }
}