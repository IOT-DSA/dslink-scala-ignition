package org.dsa.iot.rx.block

import scala.util.Try
import scala.util.control.NonFatal

import org.dsa.iot.anyToValue
import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.dslink.util.json.{ JsonArray, JsonObject }
import org.dsa.iot.jsonArrayToList
import org.dsa.iot.rx.{ Logging, NUMBER, RichJsonObject, TABLE, TEXT, TEXTAREA, aio }

trait BlockAdapter[S <: DSARxBlock] extends Logging {

  def typeName: String
  def category: String
  def makeRow: Row

  def createBlock(json: JsonObject): S
  def setupBlock(block: S, json: JsonObject, blocks: Map[String, DSARxBlock]): Unit
}

abstract class AbstractBlockAdapter[S <: DSARxBlock](
    val typeName: String, val category: String, val parameters: (String, String)*) extends BlockAdapter[S] {

  private val jsonParams = {
    def param(pName: String, pType: String) = new JsonObject(s"""{"name" : "$pName", "type" : "$pType"}""")
    val arr = new JsonArray
    parameters foreach { case (pName, pType) => arr.add(param(pName, pType)) }
    arr
  }

  val makeRow = Row.make(new Value(typeName), new Value(jsonParams), new Value(category))

  def init(port: S#Port[Value], json: JsonObject, name: String, blocks: Map[String, DSARxBlock]) =
    connect(port, json, name, blocks) orElse set(port, json, name) recover {
      case NonFatal(e) => warn(s"Error initializing port $port", e)
    }

  def connect(port: S#Port[Value], json: JsonObject, name: String, blocks: Map[String, DSARxBlock]) =
    Try(json asList name) filter (!_.isEmpty) map { list =>
      val name = getBlockName(list(0).toString)
      val source = blocks(name)
      port.bind(source)
      info(s"Port $port connected to $name.output")
    }

  def set[T](port: S#Port[Value], json: JsonObject, name: String) = Try(json.get[T](name)) map (anyToValue) map port.set

  def connect(portList: S#PortList[Value], json: JsonObject, name: String, blocks: Map[String, DSARxBlock]) =
    Try(json asList name) map { list =>
      val inbounds = list map (i => jsonArrayToList(aio[JsonArray](i)).head.toString)
      val sources = inbounds map (getBlockName(_)) map (blocks(_))
      portList.bind(sources)
    } recover {
      case NonFatal(e) => warn(s"Error initializing port list $portList", e)
    }
    
  def set(portList: S#PortList[Value], json: JsonObject, name: String) = 
    Try(json asList name) map (_ map (anyToValue)) map portList.set

  private def getBlockName(path: String) = path.drop("@parent.".length).dropRight(".output".length)
}

object BlockFactory {

  object Categories {
    val INPUT = "Input"
    val TRANSFORM = "Transform"
    val FILTER = "Filter"
    val COMBINE = "Combine"
    val AGGREGATE = "Aggregate"
  }
  import Categories._

  val adapters: Seq[BlockAdapter[_ <: DSARxBlock]] = Seq(
    DSAInputAdapter,
    IntervalAdapter,
    TimerAdapter,
    CsvFileInputAdapter,
    ScriptAdapter,
    FilterAdapter,
    SelectFirstAdapter,
    CombineLatestAdapter,
    ZipAdapter,
    WindowBySizeAdapter,
    WindowByTimeAdapter)

  val adapterMap: Map[String, BlockAdapter[_ <: DSARxBlock]] = adapters.map(a => a.typeName -> a).toMap

  /* inputs */

  object DSAInputAdapter extends AbstractBlockAdapter[DSAInput]("DSAInput", INPUT, "path" -> TEXT, "output" -> TABLE) {
    def createBlock(json: JsonObject) = DSAInput()
    def setupBlock(block: DSAInput, json: JsonObject, blocks: Map[String, DSARxBlock]) = init(block.path, json, "path", blocks)
  }

  object IntervalAdapter extends AbstractBlockAdapter[Interval]("Interval", INPUT, "initial" -> NUMBER,
    "period" -> NUMBER, "output" -> TABLE) {
    def createBlock(json: JsonObject) = Interval()
    def setupBlock(block: Interval, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      init(block.initial, json, "initial", blocks)
      init(block.period, json, "period", blocks)
    }
  }
  
  object CsvFileInputAdapter extends AbstractBlockAdapter[CsvFileInput]("CsvFileInput", INPUT,
      "path" -> TEXT, "separator" -> TEXT, "output" -> TABLE) {
    def createBlock(json: JsonObject) = CsvFileInput()
    def setupBlock(block: CsvFileInput, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      init(block.path, json, "path", blocks)
      init(block.separator, json, "separator", blocks)
      set(block.columns, json, "@array")
    }
  }

  object TimerAdapter extends AbstractBlockAdapter[Timer]("Timer", INPUT, "delay" -> NUMBER, "output" -> TABLE) {
    def createBlock(json: JsonObject) = Timer()
    def setupBlock(block: Timer, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      init(block.delay, json, "delay", blocks)
    }
  }

  /* transform */

  object WindowBySizeAdapter extends AbstractBlockAdapter[SlidingWindowBySize]("WindowBySize", TRANSFORM,
    "count" -> NUMBER, "skip" -> NUMBER, "input" -> TABLE, "output" -> TABLE) {
    def createBlock(json: JsonObject) = SlidingWindowBySize()
    def setupBlock(block: SlidingWindowBySize, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      init(block.count, json, "count", blocks)
      init(block.skip, json, "skip", blocks)
      init(block.input, json, "input", blocks)
    }
  }

  object WindowByTimeAdapter extends AbstractBlockAdapter[SlidingWindowByTime]("WindowByTime", TRANSFORM,
    "span" -> NUMBER, "shift" -> NUMBER, "input" -> TABLE, "output" -> TABLE) {
    def createBlock(json: JsonObject) = SlidingWindowByTime()
    def setupBlock(block: SlidingWindowByTime, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      init(block.span, json, "span", blocks)
      init(block.shift, json, "shift", blocks)
      init(block.input, json, "input", blocks)
    }
  }

  object ScriptAdapter extends AbstractBlockAdapter[Script]("Script", TRANSFORM, "code" -> TEXTAREA,
    "input" -> TABLE, "output" -> TABLE) {
    def createBlock(json: JsonObject) = Script()
    def setupBlock(block: Script, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      init(block.code, json, "code", blocks)
      init(block.input, json, "input", blocks)
    }
  }
  
  /* filter */

  object FilterAdapter extends AbstractBlockAdapter[Filter]("Filter", FILTER, "predicate" -> TEXTAREA,
    "input" -> TABLE, "output" -> TABLE) {
    def createBlock(json: JsonObject) = Filter()
    def setupBlock(block: Filter, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      init(block.predicate, json, "predicate", blocks)
      init(block.input, json, "input", blocks)
    }
  }

  /* combine */

  object SelectFirstAdapter extends AbstractBlockAdapter[AMB]("SelectFirst", COMBINE, "input 0" -> TABLE, "output" -> TABLE) {
    def createBlock(json: JsonObject) = AMB()
    def setupBlock(block: AMB, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      connect(block.inputs, json, "@array", blocks)
    }
  }

  object CombineLatestAdapter extends AbstractBlockAdapter[CombineLatest]("CombineLatest", COMBINE, "input 0" -> TABLE, "output" -> TABLE) {
    def createBlock(json: JsonObject) = CombineLatest()
    def setupBlock(block: CombineLatest, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      connect(block.inputs, json, "@array", blocks)
    }
  }

  object ZipAdapter extends AbstractBlockAdapter[Zip]("Zip", COMBINE, "input 0" -> TABLE, "output" -> TABLE) {
    def createBlock(json: JsonObject) = Zip()
    def setupBlock(block: Zip, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      connect(block.inputs, json, "@array", blocks)
    }
  }
}