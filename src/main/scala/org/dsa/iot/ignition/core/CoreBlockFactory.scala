package org.dsa.iot.ignition.core

import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.ignition._
import org.dsa.iot.ignition.Main.requester
import org.dsa.iot.ignition.NUMBER
import org.dsa.iot.ignition.ParamInfo.input

import com.ignition.rx.core.{ CombineLatest, Interval, TakeByCount }

/**
 * Core RX blocks.
 */
object CoreBlockFactory extends TypeConverters {

  object Categories {
    val INPUT = "Input"
    val TRANSFORM = "Transform"
    val FILTER = "Filter"
    val COMBINE = "Combine"
    val AGGREGATE = "Aggregate"
  }
  import Categories._

  /* input */

  object DSAInputAdapter extends AbstractRxBlockAdapter[DSAInput]("DSAInput", INPUT, "path" -> TEXT) {
    def createBlock(json: JsonObject) = new DSAInput
    def setupBlock(block: DSAInput, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      init(block.path, json, "path", blocks)
    }
  }

  object IntervalAdapter extends AbstractRxBlockAdapter[Interval]("Interval", INPUT,
    "initial" -> NUMBER default 0, "period" -> NUMBER default 1000) {
    def createBlock(json: JsonObject) = new Interval
    def setupBlock(block: Interval, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      init(block.initial, json, "initial", blocks)
      init(block.period, json, "period", blocks)
    }
  }

  /* transform */

  object TakeByCountAdapter extends AbstractRxBlockAdapter[TakeByCount[Any]]("TakeN", TRANSFORM,
    "count" -> NUMBER default 10, input) {
    def createBlock(json: JsonObject) = new TakeByCount[Any]
    def setupBlock(block: TakeByCount[Any], json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      init(block.count, json, "count", blocks)
      connect(block.source, json, "input", blocks)
    }
  }

  /* filter */

  object FilterAdapter extends AbstractRxBlockAdapter[ScriptFilter]("Filter", FILTER,
    "dialect" -> enum(ScriptDialect) default ScriptDialect.MVEL, "predicate" -> TEXTAREA, input) {
    def createBlock(json: JsonObject) = new ScriptFilter
    def setupBlock(block: ScriptFilter, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      set(block.dialect, json, "dialect")
      init(block.predicate, json, "predicate", blocks)
      connect(block.source, json, "input", blocks)
    }
  }

  /* combine */

  object CombineLatestAdapter extends AbstractRxBlockAdapter[CombineLatest[Any]](
    "Combine", COMBINE, "input 0" -> TABLE) {
    def createBlock(json: JsonObject) = new CombineLatest[Any]
    def setupBlock(block: CombineLatest[Any], json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      connect(block.sources, json, "@array", blocks)
    }
  }

  /* aggregate */
}