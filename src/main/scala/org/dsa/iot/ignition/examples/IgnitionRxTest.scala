package org.dsa.iot.ignition.examples

import scala.concurrent.duration.DurationInt

import org.dsa.iot.LinkMode
import org.dsa.iot.dslink.link.Requester
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.ignition.core.DSAInput
import org.dsa.iot.stringToValue

import com.ignition.rx.RichTuple2
import com.ignition.rx.core.{ CombineLatest2, Debounce, Interval, Range, Scan, TakeByCount }
import com.ignition.rx.numeric.Mul

object IgnitionRxTest extends App {

  factorial
  runningTotal

  dsa

  sys.exit

  def factorial() = {
    val prod = new Mul[Int]
    prod.output subscribe testSub("FACTORIAL")

    val rng = new Range[Int]

    rng ~> prod

    rng.range <~ (1 to 6)
    rng.reset

    rng.range <~ (1 to 3)
    rng.reset
  }

  def runningTotal() = {
    val scan = new Scan[Long, Long]
    scan.initial <~ 0
    scan.accumulator <~ ((x, y) => x + y)
    scan.output subscribe testSub("TOTAL")

    val gen = new Interval
    gen.initial <~ (10 milliseconds)
    gen.period <~ (100 milliseconds)

    val take = new TakeByCount[Long]
    take.count <~ 3

    gen ~> take ~> scan
    gen.reset
    delay(500)

    take.count <~ 4
    gen.reset
    delay(500)
  }

  def dsa() = {
    val connector = createConnector(args)

    try {
      val connection = connector start LinkMode.REQUESTER
      val requester = connection.requester

      cpuAndMemory(requester)
    } finally {
      connector.stop
    }
  }
  
  def cpuAndMemory(implicit requester: Requester) = {
    val cpu = new DSAInput
    cpu.path <~ "/downstream/System/CPU_Usage"
    
    val mem = new DSAInput
    mem.path <~ "/downstream/System/Memory_Usage"

    val dbCpu = new Debounce[Value]
    dbCpu.timeout <~ (1 second)

    val dbMem = new Debounce[Value]
    dbMem.timeout <~ (1 second)

    val combine = new CombineLatest2[Value, Value]

    (cpu ~> dbCpu, mem ~> dbMem) ~> combine

    combine.output subscribe testSub("CPU,MEM")

    cpu.reset
    mem.reset
    delay(3000)
    
    cpu.shutdown
    mem.shutdown
  }
}