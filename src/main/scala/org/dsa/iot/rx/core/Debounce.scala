package org.dsa.iot.rx.core

import scala.concurrent.duration.Duration

import org.dsa.iot.rx.RxTransformer

/**
 * Drops all values from the source that are followed by newer values before the timeout value expires.
 * 
 * <img width="640" height="310" src="https://raw.githubusercontent.com/wiki/ReactiveX/RxJava/images/rx-operators/debounce.png" alt="" /> 
 */
class Debounce[T] extends RxTransformer[T, T] {
  val timeout = Port[Duration]("timeout")
  
  protected def compute = timeout.in flatMap source.in.debounce
}

/**
 * Factory for [[Debounce]] instances.
 */
object Debounce {
  
  /**
   * Creates a new Debounce instance.
   */
  def apply[T]: Debounce[T] = new Debounce[T]
  
  /**
   * Creates a new Debounce instance with the specified timeout.
   */
  def apply[T](timeout: Duration) = {
    val block = new Debounce[T]
    block.timeout <~ timeout
    block
  }
}