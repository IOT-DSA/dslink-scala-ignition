package org.dsa.iot.ignition

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.reflect.runtime.universe
import scala.util.Try
import scala.util.control.NonFatal
import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.dslink.util.json.{ JsonArray, JsonObject }
import com.ignition.rx.RxTransformer
import com.ignition.util.Logging
import com.ignition.rx.AbstractRxBlock

/**
 * Provides information for a block's parameter.
 */
case class ParamInfo(name: String, dataType: String, defValue: Option[String] = None) {
  def default(value: String) = copy(name, dataType, Some(value))
  def default(value: Number) = copy(name, dataType, Some(value.toString))
  def default(value: Boolean) = copy(name, dataType, Some(value.toString))
  def default(value: Enumeration#Value) = copy(name, dataType, Some(value.toString))

  def toJson = {
    def inQuotes(s: String) = "\"" + s + "\""
    def field(key: String, value: String) = inQuotes(key) + " : " + inQuotes(value)
    val list = field("name", name) :: field("type", dataType) :: defValue.map(field("default", _)).toList
    new JsonObject(list.mkString("{", ", ", "}"))
  }
}

/**
 * Defines some common parameters.
 */
object ParamInfo {
  val input = ParamInfo("input", TABLE, None)
  val output = ParamInfo("output", TABLE, None)
  def input(index: Int) = ParamInfo(s"input$index", TABLE, None)
}

/**
 * A wrapper for an RxBlock, adapting it for DSA environment.
 */
trait RxBlockAdapter[S <: DSARxBlock] extends Logging {

  def name: String
  def category: String
  def parameters: Iterable[ParamInfo]

  def makeRow: Row
  def createBlock(json: JsonObject): S
  def setupBlock(block: S, json: JsonObject, blocks: Map[String, DSARxBlock]): Unit
}

/**
 * Type converter.
 */
private[ignition] class TypeAdapter[T](implicit cnv: Any => T) extends RxTransformer[Any, T] {
  protected def compute = source.in map cnv
}

/**
 * The base class for Rx block adapters.
 */
abstract class AbstractRxBlockAdapter[S <: DSARxBlock](val name: String, val category: String,
                                                       params: ParamInfo*) extends RxBlockAdapter[S] {

  val parameters = params :+ ParamInfo.output

  val makeRow = {
    val jsonParams = new JsonArray(parameters map (_.toJson) asJava)
    Row.make(new Value(name), new Value(jsonParams), new Value(category))
  }

  def init[X](port: AbstractRxBlock[_]#Port[X], json: JsonObject, name: String,
              blocks: Map[String, DSARxBlock])(implicit extractor: (JsonObject, String) => X, converter: Any => X) =
    connect(port, json, name, blocks) orElse set(port, json, name) recover {
      case NonFatal(e) => warn(s"Error initializing port $port", e)
    }

  def connect[X](port: AbstractRxBlock[_]#Port[X], json: JsonObject, name: String,
                 blocks: Map[String, DSARxBlock])(implicit cnv: Any => X) =
    Try(json asList name) filter (!_.isEmpty) map { list =>
      val name = getBlockName(list(0).toString)
      val source = blocks(name)
      val ta = new TypeAdapter[X]
      source ~> ta ~> port
      info(s"Port $port connected to $name.output")
    }

  def set[X](port: AbstractRxBlock[_]#Port[X], json: JsonObject, name: String)(implicit extractor: (JsonObject, String) => X) =
    Try(extractor(json, name)) map port.set

  def connect[X](portList: AbstractRxBlock[_]#PortList[X], json: JsonObject, name: String,
                 blocks: Map[String, DSARxBlock])(implicit cnv: Any => X) =
    Try(json asList name) map { list =>
      val inbounds = list map (i => org.dsa.iot.jsonArrayToList(aio[JsonArray](i)).head.toString)
      val sources = inbounds map (getBlockName(_)) map (blocks(_))
      portList.clear
      portList.add(sources.size)
      portList zip sources foreach {
        case (port, source) => source ~> new TypeAdapter[X] ~> port
      }
    } recover {
      case NonFatal(e) => warn(s"Error initializing port list $portList", e)
    }

  def set[T, X](portList: AbstractRxBlock[T]#PortList[X], json: JsonObject, name: String)(implicit extractor: (JsonObject, String) => Seq[X]) = {
    // portList.clear and portList.set should be doing that, will be fixed in the next version
    portList.foreach((x: AbstractRxBlock[T]#Port[X]) => x.unset)
    Try(extractor(json, name)) map (x => portList.set(x: _*))
  }

  private def getBlockName(path: String) = path.drop("@parent.".length).dropRight(".output".length)
}

/**
 * Rx block adapter factory.
 */
object RxBlockFactory {

  lazy val adapters = listAdapters[core.CoreBlockFactory.type] ++ listAdapters[spark.SparkBlockFactory.type]

  lazy val adapterMap: Map[String, RxBlockAdapter[_ <: DSARxBlock]] = adapters.map(a => a.name -> a).toMap

  private def listAdapters[TT: universe.TypeTag] = listMemberModules[TT] collect {
    case a: RxBlockAdapter[_] => a.asInstanceOf[RxBlockAdapter[_ <: DSARxBlock]]
  }
}