package org.dsa.iot.ignition

import scala.concurrent.duration.{ Duration, DurationLong }
import scala.util.Try

import org.apache.spark.sql.DataFrame
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.rx.script.ScriptDialect

/**
 * Type converters for initializing block ports.
 */
trait TypeConverters {

  /* json accessors, used by AbstractRxBlockAdapter.set() methods */

  implicit def extString(json: JsonObject, key: String) = json asString key
  implicit def extNumber(json: JsonObject, key: String) = json asNumber key
  implicit def extInt(json: JsonObject, key: String) = json asInt key
  implicit def extLong(json: JsonObject, key: String) = json asLong key
  implicit def extDouble(json: JsonObject, key: String) = json asDouble key
  implicit def extBoolean(json: JsonObject, key: String) = json asBoolean key
  implicit def extDuration(json: JsonObject, key: String) = json asDuration key
  implicit def extDialect(json: JsonObject, key: String) = json.asEnum[ScriptDialect.ScriptDialect](ScriptDialect)(key)

  implicit def extOptString(json: JsonObject, key: String) = json getAsString key
  implicit def extOptNumber(json: JsonObject, key: String) = json getAsNumber key
  implicit def extOptInt(json: JsonObject, key: String) = json getAsInt key
  implicit def extOptLong(json: JsonObject, key: String) = json getAsLong key
  implicit def extOptDouble(json: JsonObject, key: String) = json getAsDouble key
  implicit def extOptBoolean(json: JsonObject, key: String) = json getAsBoolean key
  implicit def extOptDuration(json: JsonObject, key: String) = json getAsDuration key

  implicit def extList(json: JsonObject, key: String) = json asList key
  implicit def extMap(json: JsonObject, key: String) = json asMap key

  /* port data converters, used by AbstractRxBlockAdapter.connect() methods */

  implicit def anyToValue(x: Any): Value = x match {
    case v: Value     => v
    case d: DataFrame => org.dsa.iot.scala.mapToValue(spark.dataFrameToTableData(d))
    case _            => org.dsa.iot.scala.anyToValue(x)
  }
  implicit def anyToOptValue(x: Any): Option[Value] = Option(x) map anyToValue

  implicit def anyToString(x: Any): String = x match {
    case v: Value     => org.dsa.iot.scala.valueToString(v)
    case d: DataFrame => d: String
    case _            => x.toString
  }
  implicit def anyToOptString(x: Any): Option[String] = Option(x) map anyToString

  implicit def anyToNumber(x: Any) = x match {
    case v: Value     => org.dsa.iot.scala.valueToNumber(v)
    case n: Number    => n
    case d: DataFrame => d: Number
    case s: String    => aio[Number](Try(s.toInt) getOrElse s.toDouble)
  }
  implicit def anyToOptNumber(x: Any): Option[Number] = Option(x) map anyToNumber

  implicit def anyToInt(x: Any): Int = x match {
    case v: Value     => org.dsa.iot.scala.valueToInt(v)
    case n: Number    => n.intValue
    case d: DataFrame => d: Int
    case s: String    => s.toInt
  }
  implicit def anyToOptInt(x: Any): Option[Int] = Option(x) map anyToInt

  implicit def anyToLong(x: Any): Long = x match {
    case v: Value     => org.dsa.iot.scala.valueToLong(v)
    case n: Number    => n.longValue
    case d: DataFrame => d: Long
    case s: String    => s.toLong
  }
  implicit def anyToOptLong(x: Any): Option[Long] = Option(x) map anyToLong

  implicit def anyToDouble(x: Any): Double = x match {
    case v: Value     => org.dsa.iot.scala.valueToDouble(v)
    case n: Number    => n.doubleValue
    case d: DataFrame => d: Double
    case s: String    => s.toDouble
  }
  implicit def anyToOptDouble(x: Any): Option[Double] = Option(x) map anyToDouble

  implicit def anyToBoolean(x: Any): Boolean = x match {
    case b: Boolean   => b
    case v: Value     => org.dsa.iot.scala.valueToBoolean(v)
    case n: Number    => n.doubleValue != 0.0
    case d: DataFrame => d: Boolean
    case s: String    => s.toBoolean
  }
  implicit def anyToOptBoolean(x: Any): Option[Boolean] = Option(x) map anyToBoolean

  implicit def anyToDuration(x: Any): Duration = x match {
    case d: Duration => d
    case v: Value    => org.dsa.iot.scala.valueToLong(v) milliseconds
    case n: Number   => n.longValue milliseconds
  }
  implicit def anyToOptDuration(x: Any): Option[Duration] = Option(x) map anyToDuration

  implicit def anyToDataFrame(x: Any): DataFrame = x match {
    case d: DataFrame => d
  }
  implicit def anyToOptDataFrame(x: Any): Option[DataFrame] = Option(x) map anyToDataFrame

  implicit def anyToMap(x: Any) = x match {
    case m: Map[_, _] => m.asInstanceOf[Map[String, Any]]
    case v: Value     => org.dsa.iot.scala.valueToMap(v)
  }

  implicit def anyToAny(x: Any) = x

  implicit private def scalarFromDF[T](df: DataFrame): T = (for {
    row <- if (df.rdd.isEmpty) None else Some(df.first)
    item <- if (row.size > 0) Some(row.getAs[T](0)) else None
  } yield item) getOrElse null.asInstanceOf[T]
}