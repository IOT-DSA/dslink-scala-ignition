package org.dsa.iot.rx.examples

import scala.concurrent.duration.{ Duration, DurationLong }

import org.dsa.iot.util.Logging

import rx.lang.scala.Subscriber

/**
 * Helper methods for tests.
 */
trait TestHarness extends App with Logging {
  
  /**
   * Used for testing.
   */
  case class Point(x: Int, y: Int)
  
  /**
   * Allows to use Point instances in numeric operations.
   */
  implicit object PointNumeric extends Numeric[Point] {
    def fromInt(a: Int): Point = Point(a, a)
    def minus(a: Point, b: Point): Point = Point(a.x - b.x, a.y - b.y)
    def negate(a: Point): Point = Point(-a.x, -a.y)
    def plus(a: Point, b: Point): Point = Point(a.x + b.x, a.y + b.y)
    def times(a: Point, b: Point): Point = Point(a.x * b.x, a.y + b.y)
    def toDouble(a: Point): Double = math.sqrt(a.x * a.x + a.y * a.y)
    def toFloat(a: Point): Float = toDouble(a).toFloat
    def toInt(a: Point): Int = toDouble(a).toInt
    def toLong(a: Point): Long = toDouble(a).toLong
    def compare(a: Point, b: Point): Int = toDouble(a).compare(toDouble(b))
  }

  /**
   * Creates a test subscriber with the specified name.
   */
  def testSub[T](name: String) = Subscriber[T](
    (x: T) => info(s"$name: $x (${classOf(x)})"),
    (err: Throwable) => error(s"$name: $err", err),
    () => info(s"$name: done"))

  /**
   * Delays the thread execution by `millis` milliseconds.
   */
  def delay(millis: Long): Unit = delay(millis milliseconds)

  /**
   * Delays the thread execution by the specified duration.
   */
  def delay(duration: Duration): Unit = Thread.sleep(duration.toMillis)

  /**
   * Returns the class name of the argument or "''n/a''" if the argument is `null`.
   */
  def classOf(x: Any) = if (x == null) "n/a" else x.getClass.getSimpleName

  /**
   * Prepends the test execution with the separator line and name.
   */
  def run(name: String)(body: => Unit) = {
    println("=============================================")
    println(s"Test $name")
    body
  }
}