package org.dsa.iot.rx.core

import scala.concurrent.duration.{ Duration, DurationInt }

import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.scala.Having

/**
 * Drops all values from the source that are followed by newer values before the timeout value expires.
 *
 * <img width="640" height="310" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/debounce.png" alt="" />
 */
class Debounce[T] extends RxTransformer[T, T] {

  def timeout(time: Duration): Debounce[T] = this having (timeout <~ time)

  val timeout = Port[Duration]("timeout")

  protected def compute = timeout.in flatMap source.in.debounce
}

/**
 * Factory for [[Debounce]] instances.
 */
object Debounce {

  /**
   * Creates a new Debounce instance with the timeout interval of 1 second.
   */
  def apply[T]: Debounce[T] = Debounce[T](1 second)

  /**
   * Creates a new Debounce instance with the specified timeout.
   */
  def apply[T](timeout: Duration) = {
    val block = new Debounce[T]
    block.timeout <~ timeout
    block
  }
}