package org.dsa.iot.ignition

import scala.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.{ BooleanType, DoubleType, IntegerType, StringType, StructType }
import com.ignition.types.{ Binary, TypeUtils }
import org.dsa.iot.dslink.util.json.JsonObject
import org.apache.spark.sql.types.StructField

package object spark {

  val dataTypes = List(BooleanType, StringType, IntegerType, DoubleType)
  val DATA_TYPE = enum(dataTypes map TypeUtils.nameForType: _*)

  def extStructFields(withNullable: Boolean)(json: JsonObject, key: String) =
    if (withNullable) json.asTupledList3[String, String, String]("@array") map {
      case (name, typeName, nullable) => new StructField(name, TypeUtils.typeForName(typeName), nullable.toBoolean)
    }
    else json.asTupledList2[String, String]("@array") map {
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