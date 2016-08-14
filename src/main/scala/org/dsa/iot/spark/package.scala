package org.dsa.iot

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.util.{ Random, Try }

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.{ BooleanType, DoubleType, IntegerType, StringType, StructType }
import org.dsa.iot.dslink.util.json.{ JsonArray, JsonObject }

import com.ignition.types.{ Binary, TypeUtils }

package object spark {

  /**
   * An extension to JsonObject providing usefuls Scala features.
   */
  implicit class RichJsonObject(val self: JsonObject) extends AnyVal {

    def asString(key: String) = self.get[String](key).trim
    def asNumber(key: String) = self.get[java.lang.Number](key)
    def asInt = asNumber _ andThen (_.intValue)
    def asLong = asNumber _ andThen (_.longValue)
    def asDouble = asNumber _ andThen (_.doubleValue)
    def asBoolean(key: String) = asString(key).toBoolean

    def asEnum[V <: Enumeration#Value](e: Enumeration)(key: String) = aio[V](e.withName(asString(key)))

    def getAs[T](key: String) = Option(self.get[T](key))

    def getAsString = getAs[String] _ andThen (_ flatMap noneIfEmpty)
    def getAsNumber = getAs[java.lang.Number] _
    def getAsInt = getAs[Int] _
    def getAsLong = getAs[Long] _
    def getAsDouble = getAs[Double] _
    def getAsBoolean = getAsString andThen (_ map (_.toBoolean))

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

  /* editor types */

  val TEXT = "string"
  val TEXTAREA = "textarea"
  val NUMBER = "number"
  val BOOLEAN = "boolean"
  val TABLE = "tabledata"
  def enum(values: String*): String = values.mkString("enum[", ",", "]")
  def enum(e: Enumeration): String = enum(e.values.map(_.toString).toSeq: _*)

  val dataTypes = List(BooleanType, StringType, IntegerType, DoubleType)
  val DATA_TYPE = enum(dataTypes map TypeUtils.nameForType: _*)

  /* misc */

  /**
   * Parses the string value trying to apply the type name if supplied. If the type name is missing,
   * it tries to determine the type based on the value.
   */
  def parseValue(str: String, typeName: Option[String]) = typeName map { t =>
    TypeUtils.valueOf(str, TypeUtils.typeForName(t))
  } getOrElse (Try(str.toBoolean) orElse Try(str.toInt) orElse Try(str.toDouble) getOrElse str)

  /**
   * Returns Some(str) if the argument is a non-empty string, None otherwise.
   */
  def noneIfEmpty(str: String) = Option(str) filter (!_.trim.isEmpty)

  /**
   * Splits the argument into chunks with the specified delimiter, trims each part and returns only non-empty parts.
   */
  def splitAndTrim(delim: String = ",")(str: String) = str.split(delim).map(_.trim).filterNot(_.isEmpty).toList

  /**
   * Converts a Spark DataFrame into a tabledata Map.
   */
  def dataFrameToTableData(df: DataFrame): Map[String, Any] = rddToTableData(df.rdd, Some(df.schema))

  /**
   * Converts an RDD[Row] into a tabledata Map.
   */
  def rddToTableData(rdd: RDD[Row], schema: Option[StructType] = None): Map[String, Any] =
    if (rdd.isEmpty && schema.isEmpty)
      Map("@id" -> Random.nextInt(1000), "cols" -> Nil, "rows" -> Nil)
    else {
      val columns = schema.getOrElse(rdd.first.schema).map(f => Map("name" -> f.name)).toList
      val rows = rdd.collect.map(_.toSeq.toList map sparkToSafeDSA).toList
      Map("@id" -> Random.nextInt(1000), "@type" -> "tabledata", "cols" -> columns, "rows" -> rows)
    }

  /**
   * Converts a value to a DSA-safe value.
   */
  private def sparkToSafeDSA(value: Any) = value match {
    case x: Binary         => x.mkString("[", ",", "]")
    case x: java.util.Date => x.toString
    case x @ _             => x
  }

  /**
   * A shorter notation for asInstanceOf method.
   */
  private[spark] def aio[T](obj: Any) = obj.asInstanceOf[T]
}