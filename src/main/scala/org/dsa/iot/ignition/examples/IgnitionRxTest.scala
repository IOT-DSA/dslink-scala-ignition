package org.dsa.iot.ignition.examples

import scala.concurrent.duration.DurationInt

import org.dsa.iot.dslink.link.Requester
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.ignition.core.{ DQLInput, DSAInput }
import org.dsa.iot.rx.{ RichTuple2, RxTransformer }
import org.dsa.iot.rx.core._
import org.dsa.iot.rx.numeric.Mul
import org.dsa.iot.scala.{ DSAHelper, LinkMode }

object IgnitionRxTest extends App {

  factorial
  runningTotal
  dsa
  sys.exit(0)

  def factorial() = {
    val prod = Mul[Int](true)
    prod.output subscribe testSub("FACTORIAL")

    val rng = Sequence[Int]

    rng ~> prod

    rng.items <~ (1 to 6)
    rng.reset

    rng.items <~ (1 to 3)
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
      dql(requester)
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

  def dql(implicit requester: Requester) = {
    type AnyList = List[Any]
    type AnyMap = Map[String, Any]

    val dql = new DQLInput

    val queries = List(
      """list /downstream/digi/iesA/* | filter $type="dynamic" | subscribe :name value""",
      """list /downstream/System/* | filter $type="number" | subscribe :name value""",
      """list /downstream/System/* | filter :name contains "Memory" | subscribe :name value""",
      """list /downstream/irom/atlanta/* | filter deviceType="PCT" | subscribe data/mode""")
    dql.query <~ queries(3)

    val distinct = new Distinct[AnyList]
    distinct.global <~ false
    distinct.selector <~ ((row: AnyList) => (row(0), row(1)))

    val filter = new Filter[AnyList]
    filter.predicate <~ (row => Set[Any]("heating", "cooling") contains row(2))

    val control = new RxTransformer[AnyList, (String, AnyMap)] {
      protected def compute = source.in map {
        case List(path: String, _, "cooling") => (path + "/setTemperature", Map("temperature" -> 20))
        case List(path: String, _, "heating") => (path + "/setTemperature", Map("temperature" -> 20))
      }
    }
    control.output subscribe testSub("control")

    control.output subscribe { tuple =>
      DSAHelper.invoke(tuple._1, tuple._2)
    }

    dql ~> distinct ~> filter ~> control

    dql.reset
    delay(5000)

    dql.shutdown
  }
}