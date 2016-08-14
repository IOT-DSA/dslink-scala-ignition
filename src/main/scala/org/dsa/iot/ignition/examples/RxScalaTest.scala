package org.dsa.iot.ignition.examples

import scala.collection.JavaConverters.asScalaSetConverter
import scala.concurrent.duration.DurationInt
import scala.util.Random

import org.dsa.iot.{ DSAHelper, LinkMode }
import org.dsa.iot.dslink.link.Requester
import org.dsa.iot.valueToDouble

import rx.lang.scala.Observable

object RxScalaTest extends App {

  factorial
  runningTotal
  join

  dsa

  sys.exit
  
  def factorial() = {
    val src = Observable.from(1 to 5)
    val tgt = src.product
    tgt subscribe testSub("FACTORIAL")
  }

  def runningTotal() = {
    val src = Observable.interval(100 milliseconds) map (_ => Random.nextInt(100))
    val tgt = src.scan((acc: Int, x: Int) => acc + x)
    val sub = tgt subscribe testSub("TOTAL")
    delay(500)
    sub.unsubscribe
  }

  def join() = {
    val src1 = Observable.interval(100 milliseconds) map (_ => Random.nextInt(10))
    val src2 = Observable.interval(100 milliseconds) map (_ => Random.nextInt(10))
    val tgt = (src1.distinctUntilChanged zip src2.distinctUntilChanged) filter {
      case (x, y) => x > y
    }
    val sub = tgt subscribe testSub("JOIN")
    delay(1000)
    sub.unsubscribe
  }
  
  def dsa() = {
    val connector = createConnector(args)

    try {
      val connection = connector start LinkMode.REQUESTER
      val requester = connection.requester

      cpuAndMemory(requester)
      weatherAlert(requester)
    } finally {
      connector.stop
    }
  }  

  def cpuAndMemory(implicit requester: Requester) = {
    val cpu = DSAHelper watch "/downstream/System/CPU_Usage" map (_.getValue: Double)
    val mem = DSAHelper watch "/downstream/System/Memory_Usage" map (_.getValue: Double)

    val cpuMem = cpu.debounce(1 second) combineLatest mem.debounce(1 second)

    val avgCpu3 = cpu slidingBuffer (3, 1) map {
      case Nil => 0
      case v   => v.sum / v.size
    }

    val subCpuMem = cpuMem subscribe testSub("CPU,MEM")
    val subAvgCpu3 = avgCpu3 subscribe testSub("AVG(3CPU)")

    delay(5000)
    subCpuMem.unsubscribe
    subAvgCpu3.unsubscribe
  }

  def weatherAlert(implicit requester: Requester) = {
    val cities = DSAHelper list ("/downstream/Weather") flatMapIterable (_.getUpdates.keySet.asScala) map (_.getPath)

    val atlanta = (cities filter (_.toLowerCase contains "atlanta")).toBlocking first

    val temperature = DSAHelper watch s"$atlanta/Temperature" map (_.getValue: Double)
    val lowTemperature = temperature filter (_ < 32) map (t => s"Low temperature: $t")

    val wind = DSAHelper watch s"$atlanta/Wind_Speed" map (_.getValue: Double)
    val highWind = wind filter (_ > 40) map (w => s"High wind: $w")

    val alerts = lowTemperature merge highWind

    val sub1 = temperature subscribe testSub("TEMPERATURE")
    val sub2 = wind subscribe testSub("WIND")
    val sub3 = alerts subscribe testSub("ALERTS")

    delay(5000)
    sub1.unsubscribe
    sub2.unsubscribe
    sub3.unsubscribe
  }
}