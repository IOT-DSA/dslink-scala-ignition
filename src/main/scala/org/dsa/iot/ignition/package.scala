package org.dsa.iot

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.concurrent.duration.DurationLong
import scala.reflect.runtime.universe
import scala.util.Try

import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.value.{ Value, ValueType }
import org.dsa.iot.dslink.util.json.{ JsonArray, JsonObject }
import org.dsa.iot.ignition.Settings

import com.ignition.rx.AbstractRxBlock

/**
 * Common types and helper functions.
 */
package object ignition {
  import Settings._

  type DSARxBlock = AbstractRxBlock[_]
  type DSABlockMap = Map[String, DSARxBlock]

  /**
   * An extension to JsonObject providing usefuls Scala features.
   */
  implicit class RichJsonObject(val self: JsonObject) extends AnyVal {

    def asString(key: String) = self.get[String](key).trim
    def asNumber(key: String) = self.get[java.lang.Number](key)
    def asInt = asNumber _ andThen (_.intValue)
    def asLong = asNumber _ andThen (_.longValue)
    def asDouble = asNumber _ andThen (_.doubleValue)
    def asBoolean = asString _ andThen (_.toBoolean)
    def asDuration = asLong andThen (_ milliseconds)
    def asEnum[V <: Enumeration#Value](e: Enumeration)(key: String) = aio[V](e.withName(asString(key)))

    def getAs[T](key: String) = Option(self.get[T](key))

    def getAsString = getAs[String] _ andThen (_ flatMap noneIfEmpty)
    def getAsNumber = getAs[java.lang.Number] _
    def getAsInt = getAs[Int] _
    def getAsLong = getAs[Long] _
    def getAsDouble = getAs[Double] _
    def getAsBoolean = getAsString andThen (_ map (_.toBoolean))
    def getAsDuration = getAsLong andThen (_ map (_ milliseconds))

    def asList(key: String) = getAs[JsonArray](key) map (_.asScala.map(x => x: Any).toList) getOrElse Nil

    def asStringList = asList _ andThen (_ map (_.toString))
    def asNumberList = asList _ andThen (_ map (_.asInstanceOf[java.lang.Number]))
    def asIntList = asNumberList andThen (_ map (_.intValue))
    def asDoubleList = asNumberList andThen (_ map (_.doubleValue))
    def asBooleanList = asList _ andThen (_ map (_.asInstanceOf[Boolean]))

    def asTupledList2[T1, T2] = asList _ andThen partition(2) andThen (_ map listToTuple2[T1, T2] toList)
    def asTupledList3[T1, T2, T3] = asList _ andThen partition(3) andThen (_ map listToTuple3[T1, T2, T3] toList)
    def asTupledList4[T1, T2, T3, T4] = asList _ andThen partition(4) andThen (_ map listToTuple4[T1, T2, T3, T4] toList)

    private def partition(size: Int)(list: Iterable[Any]) = list grouped size filterNot (_.forall {
      case null                        => true
      case x: String if x.trim.isEmpty => true
      case _                           => false
    })

    private def listToTuple2[T1, T2](it: TraversableOnce[Any]) = it match {
      case List(e1, e2) => (aio[T1](e1), aio[T2](e2))
    }

    private def listToTuple3[T1, T2, T3](it: TraversableOnce[Any]) = it match {
      case List(e1, e2, e3) => (aio[T1](e1), aio[T2](e2), aio[T3](e3))
    }

    private def listToTuple4[T1, T2, T3, T4](it: TraversableOnce[Any]) = it match {
      case List(e1, e2, e3, e4) => (aio[T1](e1), aio[T2](e2), aio[T3](e3), aio[T4](e4))
    }
  }

  /**
   * An extension to Value providing some handy accessors.
   */
  implicit class RichValue(val self: Value) extends AnyVal {
    def getNumber = self.getType match {
      case ValueType.NUMBER => self.getNumber
      case ValueType.BOOL   => if (self.getBool) Int.box(1) else Int.box(0)
      case ValueType.STRING => Try(Int.box(self.getString.toInt)) getOrElse (Double.box(self.getString.toDouble))
    }
    def getInt = getNumber.intValue
    def getLong = getNumber.longValue
    def getDouble = getNumber.doubleValue
    def getBoolean = self.getType match {
      case ValueType.BOOL   => self.getBool: Boolean
      case ValueType.NUMBER => self.getNumber.intValue != 0
      case ValueType.STRING => self.getString.toBoolean
    }
    def getString = self.getType match {
      case ValueType.STRING => self.getString
      case ValueType.NUMBER => self.getNumber.toString
      case ValueType.BOOL   => self.getBool.toString
      case _                => self.toString
    }

    def getList = self.getArray.getList

    def getMap = self.getMap.getMap

    override def toString = self.toString
  }

  /* editor types */

  val TEXT = "string"
  val TEXTAREA = "textarea"
  val NUMBER = "number"
  val BOOLEAN = "boolean"
  val TABLE = "tabledata"
  val LIST = "list"
  def enum(values: String*): String = values.mkString("enum[", ",", "]")
  def enum(e: Enumeration): String = enum(e.values.map(_.toString).toSeq: _*)

  /* type helpers */

  implicit def tuple2Param(pair: (String, String)) = ParamInfo(pair._1, pair._2, None)

  /* node helpers */

  private val NODE_TYPE = "nodeType"
  private val FLOW = "flow"

  /**
   * Creates a new flow node.
   */
  def createFlowNode(parent: Node, name: String) =
    parent createChild name config (NODE_TYPE -> FLOW, dfDesignerKey -> s"$dfPath/$name") build ()

  /**
   * Returns the type of the node.
   */
  def getNodeType(node: Node) = node.configurations.get(NODE_TYPE) map (_.asInstanceOf[String])

  /**
   * Checks if the node type is flow.
   */
  def isFlowNode(node: Node) = getNodeType(node) == Some(FLOW)

  /* misc */

  /**
   * Lists objects defined in the scope of the specified type.
   */
  def listMemberModules[TT: universe.TypeTag] = {
    val m = universe.runtimeMirror(getClass.getClassLoader)
    universe.typeOf[TT].decls filter (_.isModule) map { obj =>
      m.reflectModule(obj.asModule).instance
    }
  }

  /**
   * A shorter notation for asInstanceOf method.
   */
  def aio[T](obj: Any) = obj.asInstanceOf[T]

  /**
   * Returns Some(str) if the argument is a non-empty string, None otherwise.
   */
  def noneIfEmpty(str: String) = Option(str) filter (!_.trim.isEmpty)
}