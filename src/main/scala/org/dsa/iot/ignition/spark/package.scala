package org.dsa.iot.ignition

import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._
import org.dsa.iot.dslink.util.json.JsonObject

import com.ignition.frame.{ FrameProducer, SparkRuntime }
import com.ignition.types.{ Binary, TypeUtils }

package object spark {

  val dataTypes = List(BooleanType, StringType, IntegerType, DoubleType)
  val DATA_TYPE = enum(dataTypes map TypeUtils.nameForType: _*)
  
  /**
   * Parses the string value using the supplied type name.
   */
  def parseValue(str: String, typeName: String) = TypeUtils.valueOf(str, TypeUtils.typeForName(typeName))

  /**
   * Creates a producer, wrapper for the data frame.
   */
  def producer(df: DataFrame) = new FrameProducer { self =>
    protected def compute(implicit runtime: SparkRuntime) = optLimit(df, runtime.previewMode)
    def toXml: scala.xml.Elem = ???
    def toJson: org.json4s.JValue = ???
  }

  /**
   * Extracts a string from JSON and splits into a list of strings using the separator.
   */
  val extractSeparatedStrings: (JsonObject, String) => List[String] = extractSeparatedStrings("\\s*,\\s*")

  /**
   * Extracts a string from JSON and splits into a list of strings using the separator.
   */
  def extractSeparatedStrings(separator: String)(json: JsonObject, key: String): List[String] =
    json getAsString key map splitAndTrim(separator) getOrElse Nil

  /**
   * Extracts a list of `StructFields` elements from JSON.
   */
  def extractStructFields(withNullable: Boolean)(json: JsonObject, key: String) =
    if (withNullable) json.asTupledList3[String, String, String](key) map {
      case (name, typeName, nullable) => new StructField(name, TypeUtils.typeForName(typeName), nullable.toBoolean)
    }
    else json.asTupledList2[String, String](key) map {
      case (name, typeName) => new StructField(name, TypeUtils.typeForName(typeName), true)
    }

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
      val rows = rdd.collect.map(_.toSeq.toList map toSafeDSAValue).toList
      Map("@id" -> Random.nextInt(1000), "@type" -> "tabledata", "cols" -> columns, "rows" -> rows)
    }

  /**
   * Converts a value to a DSA-safe value.
   */
  private def toSafeDSAValue(value: Any) = value match {
    case x: Binary         => x.mkString("[", ",", "]")
    case x: java.util.Date => x.toString
    case x @ _             => x
  }
}