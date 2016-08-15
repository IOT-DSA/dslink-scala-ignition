package org.dsa.iot.ignition

import scala.concurrent.duration.{ Duration, DurationLong }
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.dslink.util.json.JsonObject
import scala.util.Try
import scala.math.Numeric.DoubleIsFractional
import scala.math.Numeric.IntIsIntegral
import spire.math.FloatIsFractional
import scala.math.Numeric.FloatIsFractional
import scala.math.Numeric.LongIsIntegral

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
  implicit def anyToNumber(x: Any) = x match {
    case v: Value  => org.dsa.iot.valueToNumber(v)
    case n: Number => n
    case s: String => aio[Number](Try(s.toInt) getOrElse s.toDouble)
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

  /* numeric */

  implicit object RichValueNumeric extends Numeric[RichValue] {
    def plus(x: RichValue, y: RichValue): RichValue =
      handle(x.getNumber, y.getNumber, DoubleIsFractional.plus, IntIsIntegral.plus)
    def minus(x: RichValue, y: RichValue): RichValue =
      handle(x.getNumber, y.getNumber, DoubleIsFractional.minus, IntIsIntegral.minus)
    def times(x: RichValue, y: RichValue): RichValue =
      handle(x.getNumber, y.getNumber, DoubleIsFractional.times, IntIsIntegral.times)
    def negate(x: RichValue): RichValue = handle(x.getNumber, DoubleIsFractional.negate, IntIsIntegral.negate)
    def fromInt(x: Int): RichValue = x
    def toInt(x: RichValue): Int = x.getInt
    def toLong(x: RichValue): Long = x.getLong
    def toFloat(x: RichValue): Float = x.getNumber.floatValue
    def toDouble(x: RichValue): Double = x.getDouble
    def compare(x: RichValue, y: RichValue): Int = x.getDouble.compare(y.getDouble)

    private def handle(a: Number, dblOp: Double => Double, intOp: Int => Int) = {
      if (a.isInstanceOf[Double] || a.isInstanceOf[Float])
        dblOp(a.doubleValue)
      else
        intOp(a.intValue)
    }

    private def handle(a: Number, b: Number, dblOp: (Double, Double) => Double,
                       intOp: (Int, Int) => Int) = {
      if (a.isInstanceOf[Double] || b.isInstanceOf[Double] || a.isInstanceOf[Float] || b.isInstanceOf[Float])
        dblOp(a.doubleValue, b.doubleValue)
      else
        intOp(a.intValue, b.intValue)
    }
  }
}