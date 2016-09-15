package org.dsa.iot.ignition

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.util.Try
import scala.util.control.NonFatal

import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.dslink.util.json.{ JsonArray, JsonObject }
import org.dsa.iot.rx.{ AbstractRxBlock, RxTransformer }
import org.dsa.iot.util.Logging

/**
 * Provides information for a block's parameter.
 *
 * @param name parameter name.
 * @param dataType parameter data type.
 * @param defValue optional default value for the parameter.
 */
case class ParamInfo(name: String, dataType: String, defValue: Option[Any] = None) {
  
  def default(value: Any) = copy(defValue = Some(value))

  def toJson = {
    def inQuotes(s: String) = "\"" + s + "\""
    def field(key: String, value: Any) = inQuotes(key) + " : " + (value match {
      case x: Number  => x.toString
      case x: Boolean => x.toString
      case x @ _      => inQuotes(x.toString)
    })
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

  /**
   * Block name.
   */
  def name: String

  /**
   * Block category (like Input, Transform, Aggregate, etc.)
   */
  def category: String

  /**
   * Block parameters/connection points (excluding Output, which is added automatically).
   */
  def parameters: Iterable[ParamInfo]

  /**
   * Creates the block description row.
   */
  def makeRow: Row

  /**
   * Creates a new block from JSON. It is separated from `setupBlock` to take advantage of
   * the dynamic configuration of Ignition RX.
   */
  def createBlock(json: JsonObject): S

  /**
   * Configures the existing block from JSON.It is separated from `createBlock` to take advantage
   * of the dynamic configuration of Ignition RX.
   */
  def setupBlock(block: S, json: JsonObject, blocks: DSABlockMap): Unit
}

/**
 * Type converter used to bind connection points that do not have the same data type.
 */
private[ignition] class TypeAdapter[T](implicit cnv: Any => T) extends RxTransformer[Any, T] {
  protected def compute = source.in map cnv
}

/**
 * The base class for Rx block adapters.
 */
abstract class AbstractRxBlockAdapter[S <: DSARxBlock](val name: String, val category: String,
                                                       params: ParamInfo*) extends RxBlockAdapter[S] {
  
  /**
   * The name of the field that stores list-type attribute values.
   */
  val arrayField = "@array"
  
  /**
   * The specified block parameters plus output.
   */
  val parameters = params :+ ParamInfo.output

  /**
   * Creates the block description row by concatenating the name, parameters, and category.
   */
  val makeRow = {
    val jsonParams = new JsonArray(parameters map (_.toJson) asJava)
    Row.make(new Value(name), new Value(jsonParams), new Value(category))
  }

  /**
   * Initializes the specified port by trying to either connect it to another block (if available),
   * or to set its value explicitly.
   */
  def init[X](port: S#Port[X], json: JsonObject, name: String,
              blocks: DSABlockMap)(implicit extractor: (JsonObject, String) => X, converter: Any => X) = {
    if (json contains name) {
      tryConnecting(port, json, name, blocks) orElse trySetting(port, json, name) recover {
        case NonFatal(e) => warn(s"Error initializing $port", e)
      } get
    } else
      debug(s"No connection or value specified for $port under [$name]")
  }

  /**
   * Connects the specified port to the output of another block. If the connection cannot be made,
   * prints an error message.
   */
  def connect[X](port: S#Port[X], json: JsonObject, name: String,
                 blocks: DSABlockMap)(implicit cnv: Any => X) = {
    if (json contains name)
      tryConnecting(port, json, name, blocks) getOrElse error(s"Error connecting $port under [$name]")
    else
      debug(s"No connection specified for $port under [$name]")
  }

  /**
   * Tries connecting the specified port to the output of another block, referenced by JSON.
   */
  private def tryConnecting[X](port: S#Port[X], json: JsonObject, name: String,
                               blocks: DSABlockMap)(implicit cnv: Any => X) =
    Try(json asList name) filter (!_.isEmpty) map { list =>
      val name = getBlockName(list(0).toString)
      val source = blocks(name)
      val ta = new TypeAdapter[X]
      source ~> ta
      port <~ ta
      info(s"$port connected to $name.output")
    }

  /**
   * Sets the specified port to an explicit value. If the value cannot be set, prints an error message.
   */
  def set[X](port: S#Port[X], json: JsonObject,
             name: String)(implicit extractor: (JsonObject, String) => X) = {
    if (json contains name)
      trySetting(port, json, name) getOrElse error(s"Error setting $port under [$name]")
    else
      debug(s"No value specified for $port under [$name]")
  }

  /**
   * Tries to set the specified port to a value extracted from JSON.
   */
  private def trySetting[X](port: S#Port[X], json: JsonObject,
                            name: String)(implicit extractor: (JsonObject, String) => X) =
    Try(extractor(json, name)) map { value =>
      port <~ value
      info(s"$port set to $value")
    }

  /**
   * Tries to connect the port list to a set of other blocks' outputs.
   */
  def connect[X](portList: AbstractRxBlock[_]#PortList[X], json: JsonObject, name: String,
                 blocks: DSABlockMap)(implicit cnv: Any => X) =
    Try(json asList name) map { list =>
      val inbounds = list map (i => i.asInstanceOf[Seq[_]]) map (_.head.toString)
      val sources = inbounds map (getBlockName(_)) map (blocks(_))
      portList.clear
      portList.add(sources.size)
      portList zip sources foreach {
        case (port, source) => source ~> new TypeAdapter[X] ~> port
      }
      info(s"""PortList $portList connected to output(s) of ${inbounds.mkString("(", ",", ")")}""")
    } recover {
      case NonFatal(e) => warn(s"Error initializing port list $portList", e)
    } get

  /**
   * Tries to set the ports of the specified portList to values extracted from JSON.
   */
  def set[T, X](portList: AbstractRxBlock[T]#PortList[X], json: JsonObject, name: String)(implicit extractor: (JsonObject, String) => Seq[X]) = {
    portList.clear
    Try(extractor(json, name)) map (x => portList.set(x: _*))
  } get

  private def getBlockName(path: String) = path.drop("@parent.".length).dropRight(".output".length)
}