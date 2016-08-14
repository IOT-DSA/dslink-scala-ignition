package org.dsa.iot.ignition

import scala.concurrent.duration.{ Duration, DurationLong }

import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.dslink.util.json.JsonObject

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

  /* port data converters, used by AbstractRxBlockAdapter.connect() methods */
  
  implicit def anyToValue(x: Any): RichValue = x match {
    case v: Value => v
    case _        => org.dsa.iot.anyToValue(x)
  }
  implicit def anyToString(x: Any) = x match {
    case v: Value => org.dsa.iot.valueToString(v)
    case _        => x.toString
  }
  implicit def anyToInt(x: Any) = x match {
    case v: Value  => org.dsa.iot.valueToInt(v)
    case n: Number => n.intValue
    case s: String => s.toInt
  }
  implicit def anyToLong(x: Any) = x match {
    case v: Value  => org.dsa.iot.valueToLong(v)
    case n: Number => n.longValue
    case s: String => s.toLong
  }
  implicit def anyToDouble(x: Any) = x match {
    case v: Value  => org.dsa.iot.valueToDouble(v)
    case n: Number => n.doubleValue
    case s: String => s.toDouble
  }
  implicit def anyToBoolean(x: Any) = x match {
    case b: Boolean => b
    case v: Value   => org.dsa.iot.valueToBoolean(v)
    case n: Number  => n.doubleValue != 0.0
    case s: String  => s.toBoolean
  }
  implicit def anyToDuration(x: Any) = x match {
    case d: Duration => d
    case v: Value    => org.dsa.iot.valueToLong(v) milliseconds
    case n: Number   => n.longValue milliseconds
  }
  implicit def anyToAny(x: Any) = x
}