package org.dsa.iot.rx.core

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Creates an Observable which produces buffers of collected values.
 *
 * It produces a new buffer of at most `count` size every `skip` values. Therefore if `count == skip`,
 * it produces non-overlapping buffers.
 */
class WindowBySize[T] extends RxTransformer[T, Seq[T]] {

  def count(n: Int): WindowBySize[T] = this having (count <~ n)
  def skip(n: Int): WindowBySize[T] = this having (skip <~ n)

  val count = Port[Int]("count")
  val skip = Port[Int]("skip")

  protected def compute = (count.in combineLatest skip.in) flatMap {
    case (cnt, skp) if cnt == skp => source.in.tumblingBuffer(cnt)
    case (cnt, skp)               => source.in.slidingBuffer(cnt, skp)
  }
}

/**
 * Factory for [[WindowBySize]] instances.
 */
object WindowBySize {

  /**
   * Creates a new WindowBySize instance of overlapping buffers of 10 elements with shift of 1.
   */
  def apply[T]: WindowBySize[T] = WindowBySize(10, 1)

  /**
   * Creates a new WindowBySize instance for non-overlapping buffers of size `count`.
   */
  def apply[T](count: Int): WindowBySize[T] = WindowBySize(count, count)

  /**
   * Creates a new WindowBySize instance for buffers of size `count` created every `skip` items.
   */
  def apply[T](count: Int, skip: Int): WindowBySize[T] = {
    val block = new WindowBySize[T]
    block.count <~ count
    block.skip <~ skip
    block
  }
}