package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 *  Drops a specified number of items from the beginning or end of source sequence.
 *
 *  <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/skip.png" alt="" />
 *  <img width="640" height="305" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/skipLast.png" alt="" />
 */
class DropByCount[T] extends RxTransformer[T, T] {

  def fromRight(): DropByCount[T] = this having (right <~ true)
  def fromLeft(): DropByCount[T] = this having (right <~ false)
  def count(n: Int): DropByCount[T] = this having (count <~ n)

  val right = Port[Boolean]("right")
  val count = Port[Int]("count")

  protected def compute = right.in combineLatest count.in flatMap {
    case (true, c)  => source.in.dropRight(c)
    case (false, c) => source.in.drop(c)
  }
}

/**
 * Factory for [[DropByCount]] instances.
 */
object DropByCount {

  /**
   * Creates a new DropByCount instance for dropping 1 element from the beginning
   * of the sequence.
   */
  def apply[T]: DropByCount[T] = DropByCount(1, false)

  /**
   * Creates a new DropByCount instance for the given item count. If `right` is true, drops items
   * from the end of the sequence, otherwise drops items from the beginning.
   */
  def apply[T](count: Int, right: Boolean): DropByCount[T] = {
    val block = new DropByCount[T]
    block.right <~ right
    block.count <~ count
    block
  }
}