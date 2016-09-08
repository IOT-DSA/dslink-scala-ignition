package org.dsa.iot.rx.core

import scala.concurrent.duration.Duration
import scala.util.Random

import org.dsa.iot.rx.AbstractRxBlock

import rx.lang.scala.Observable

/**
 * Emits `0`, `1`, `2`, `...` with a delay, randomly chosen for each item.
 */
class RandomInterval(gaussian: Boolean) extends AbstractRxBlock[Long] {
  val min = Port[Duration]("min")
  val max = Port[Duration]("max")

  private val delayFunc = if (gaussian) nextGaussian _ else nextUniform _

  protected def compute = (min.in combineLatest max.in) flatMap {
    case (mn, mx) => Observable.apply { sub =>
      val stopFlag = new java.util.concurrent.atomic.AtomicBoolean(false)
      val counter = new java.util.concurrent.atomic.AtomicLong(0)
      val thread = new Thread {
        override def run() = try {
          while (!stopFlag.get) {
            val delay = delayFunc(mn.toMillis, mx.toMillis)
            Thread.sleep(delay)
            sub.onNext(counter.getAndIncrement)
          }
        } catch {
          case e: InterruptedException => debug("Thread loop interruped")
        }
      }
      thread.start
      sub.add {
        stopFlag.set(true)
        thread.interrupt
      }
    }
  }

  private def nextUniform(min: Long, max: Long) = Random.nextInt((max - min + 1).toInt) + min

  private def nextGaussian(min: Long, max: Long) = {
    val multiplier = (max - min) / 6.0
    val mean = (max + min) / 2
    math.min(math.max(Random.nextGaussian * multiplier + mean, min), max).toLong
  }
}

/**
 * Factory for [[RandomInterval]] instances.
 */
object RandomInterval {

  /**
   * Creates a new RandomInterval instance.
   */
  def apply(gaussian: Boolean): RandomInterval = new RandomInterval(gaussian)

  /**
   * Creates a new RandomInterval instance with given minimum and maximum delay and flag showing
   * whether a uniform or gaussian (normal) distribution should be used.
   */
  def apply(min: Duration, max: Duration, gaussian: Boolean = false): RandomInterval = {
    val block = new RandomInterval(gaussian)
    block.min <~ min
    block.max <~ max
    block
  }
}