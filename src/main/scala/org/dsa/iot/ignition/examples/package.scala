package org.dsa.iot.ignition

import scala.concurrent.duration.{ Duration, DurationLong }

import org.dsa.iot.scala.{ DSAConnector, Having }

import com.ignition.util.Logging

import rx.lang.scala.Subscriber

package object examples extends Logging {

  val DEFAULT_BROKER_URL = "http://localhost:8080/conn"

  /**
   * Creates a new DSAConnector.
   */
  private[examples] def createConnector(args: Array[String]) = {
    val brokerUrl = if (args.length < 1)
      DEFAULT_BROKER_URL having println(s"Broker URL not specified, using the default one: $DEFAULT_BROKER_URL")
    else
      args(0) having (x => println(s"Broker URL: $x"))

    DSAConnector("-b", brokerUrl)
  }

  private[examples] def testSub[T](name: String) = Subscriber[T](
    (x: T) => info(s"$name: $x"),
    (err: Throwable) => error(s"$name: $err", err),
    () => info(s"$name: done"))

  private[examples] def delay(millis: Long): Unit = delay(millis milliseconds)

  private[examples] def delay(duration: Duration): Unit = Thread.sleep(duration.toMillis)
}