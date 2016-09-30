package org.dsa.iot.rx.core

import scala.concurrent.duration.{ Duration, DurationInt }
import scala.util.Random

import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.Having

import rx.lang.scala.Observable

/**
 * Emits `0`, `1`, `2`, `...` with a delay, randomly chosen for each item.
 */
class RandomInterval extends AbstractRxBlock[Long] {

  def normal(): RandomInterval = this having (gaussian <~ true)
  def uniform(): RandomInterval = this having (gaussian <~ false)
  def min(time: Duration): RandomInterval = this having (min <~ time)
  def max(time: Duration): RandomInterval = this having (max <~ time)

  val gaussian = Port[Boolean]("gaussian")
  val min = Port[Duration]("min")
  val max = Port[Duration]("max")

  protected def compute = (min.in combineLatest max.in combineLatest gaussian.in) flatMap {
    case ((mn, mx), gs) => Observable.apply { sub =>
      val delayFunc = if (gs) nextGaussian _ else nextUniform _
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
   * Creates a new RandomInterval instance with the each delay ''uniformly'' distributed
   * between '''one''' and '''ten''' seconds.
   */
  def apply(): RandomInterval = RandomInterval(1 second, 10 seconds, false)

  /**
   * Creates a new RandomInterval instance with given minimum and maximum delay and flag showing
   * whether a uniform or gaussian (normal) distribution should be used.
   */
  def apply(min: Duration, max: Duration, gaussian: Boolean = false): RandomInterval = {
    val block = new RandomInterval
    block.gaussian <~ gaussian
    block.min <~ min
    block.max <~ max
    block
  }
}