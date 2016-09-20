package org.dsa.iot.ignition.core

import scala.reflect.runtime.universe

import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.ignition._
import org.dsa.iot.ignition.Main.requester
import org.dsa.iot.ignition.NUMBER
import org.dsa.iot.ignition.ParamInfo.input
import org.dsa.iot.rx.core._
import org.dsa.iot.rx.numeric._
import org.dsa.iot.rx.script.{ ScriptDialect, ScriptFilter, ScriptTransform, ScriptCount }

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
    def createBlock(json: JsonObject) = DSAInput()
    def setupBlock(block: DSAInput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.path, json, "path", blocks)
    }
  }

  object DQLInputAdapter extends AbstractRxBlockAdapter[DQLInput]("DQLInput", INPUT, "query" -> TEXTAREA) {
    def createBlock(json: JsonObject) = DQLInput()
    def setupBlock(block: DQLInput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.query, json, "query", blocks)
    }
  }

  object IntervalAdapter extends AbstractRxBlockAdapter[Interval]("Interval", INPUT,
    "initial" -> NUMBER default 0, "period" -> NUMBER default 1000) {
    def createBlock(json: JsonObject) = Interval()
    def setupBlock(block: Interval, json: JsonObject, blocks: DSABlockMap) = {
      init(block.initial, json, "initial", blocks)
      init(block.period, json, "period", blocks)
    }
  }

  object RandomIntervalAdapter extends AbstractRxBlockAdapter[RandomInterval]("RandomInterval", INPUT,
    "normal" -> BOOLEAN default false, "min" -> NUMBER default 1000, "max" -> NUMBER default 5000) {
    def createBlock(json: JsonObject) = RandomInterval(json asBoolean "normal")
    def setupBlock(block: RandomInterval, json: JsonObject, blocks: DSABlockMap) = {
      init(block.min, json, "min", blocks)
      init(block.max, json, "max", blocks)
    }
  }

  object RangeAdapter extends AbstractRxBlockAdapter[Range[Int]]("Range", INPUT,
    "start" -> NUMBER default 0, "end" -> NUMBER default 10, "step" -> NUMBER default 1) {
    def createBlock(json: JsonObject) = Range[Int]
    def setupBlock(block: Range[Int], json: JsonObject, blocks: DSABlockMap) = {
      init(block.begin, json, "start", blocks)
      init(block.end, json, "end", blocks)
      init(block.step, json, "step", blocks)
    }
  }

  object TimerAdapter extends AbstractRxBlockAdapter[Timer]("Timer", INPUT, "delay" -> NUMBER default 10000) {
    def createBlock(json: JsonObject) = Timer()
    def setupBlock(block: Timer, json: JsonObject, blocks: DSABlockMap) = {
      init(block.delay, json, "delay", blocks)
    }
  }

  /* transform */

  object DelayAdapter extends TransformerAdapter[Any, Delay[Any]](
    "Delay", TRANSFORM, "period" -> NUMBER default 1000) {
    def createBlock(json: JsonObject) = Delay[Any]
    def setupAttributes(block: Delay[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.period, json, "period", blocks)
    }
  }

  object TakeByCountAdapter extends TransformerAdapter[Any, TakeByCount[Any]]("TakeBySize", TRANSFORM,
    "count" -> NUMBER default 10) {
    def createBlock(json: JsonObject) = TakeByCount[Any]
    def setupAttributes(block: TakeByCount[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.count, json, "count", blocks)
    }
  }

  object TakeByTimeAdapter extends TransformerAdapter[Any, TakeByTime[Any]]("TakeByTime", TRANSFORM,
    "period" -> NUMBER default 10000) {
    def createBlock(json: JsonObject) = TakeByTime[Any]
    def setupAttributes(block: TakeByTime[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.period, json, "period", blocks)
    }
  }

  object TakeRightAdapter extends TransformerAdapter[Any, TakeRight[Any]]("TakeRight", TRANSFORM,
    "period" -> NUMBER default 10000, "count" -> NUMBER default 10) {
    def createBlock(json: JsonObject) = TakeRight[Any]
    def setupAttributes(block: TakeRight[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.period, json, "period", blocks)
      init(block.count, json, "count", blocks)
    }
  }
  
  object DropByCountAdapter extends TransformerAdapter[Any, DropByCount[Any]]("DropBySize", TRANSFORM,
    "right" -> BOOLEAN default false, "count" -> NUMBER default 10) {
    def createBlock(json: JsonObject) = DropByCount[Any](json asBoolean "right")
    def setupAttributes(block: DropByCount[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.count, json, "count", blocks)
    }
  }
  
  object DropByTimeAdapter extends TransformerAdapter[Any, DropByTime[Any]]("DropByTime", TRANSFORM,
    "right" -> BOOLEAN default false, "period" -> NUMBER default 10000) {
    def createBlock(json: JsonObject) = DropByTime[Any](json asBoolean "right")
    def setupAttributes(block: DropByTime[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.period, json, "period", blocks)
    }
  }

  object WindowByTimeAdapter extends TransformerAdapter[Any, WindowByTime[Any]]("WindowByTime",
    TRANSFORM, "span" -> NUMBER default 10000, "shift" -> NUMBER default 1000) {
    def createBlock(json: JsonObject) = WindowByTime[Any]
    def setupAttributes(block: WindowByTime[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.span, json, "span", blocks)
      init(block.shift, json, "shift", blocks)
    }
  }

  object WindowBySizeAdapter extends TransformerAdapter[Any, WindowBySize[Any]]("WindowBySize",
    TRANSFORM, "count" -> NUMBER default 10, "skip" -> NUMBER default 1) {
    def createBlock(json: JsonObject) = WindowBySize[Any]
    def setupAttributes(block: WindowBySize[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.count, json, "count", blocks)
      init(block.skip, json, "skip", blocks)
    }
  }

  object TransformAdapter extends TransformerAdapter[Any, ScriptTransform[Any, AnyRef]]("Transform", TRANSFORM,
    "dialect" -> enum(ScriptDialect) default ScriptDialect.MVEL, "script" -> TEXTAREA) {
    def createBlock(json: JsonObject) = ScriptTransform[Any, AnyRef]
    def setupAttributes(block: ScriptTransform[Any, AnyRef], json: JsonObject, blocks: DSABlockMap) = {
      set(block.dialect, json, "dialect")
      init(block.script, json, "script", blocks)
    }
  }

  object ZipWithIndexAdapter extends TransformerAdapter[Any, ZipWithIndex[Any]]("ZipWithIndex", TRANSFORM) {
    def createBlock(json: JsonObject) = ZipWithIndex[Any]
    def setupAttributes(block: ZipWithIndex[Any], json: JsonObject, blocks: DSABlockMap) = {}
  }

  object DSAInvokeAdapter extends TransformerAdapter[Value, DSAInvoke]("DSAInvoke", TRANSFORM) {
    def createBlock(json: JsonObject) = DSAInvoke()
    def setupAttributes(block: DSAInvoke, json: JsonObject, blocks: DSABlockMap) = {}
  }

  /* filter */

  object FilterAdapter extends TransformerAdapter[Any, ScriptFilter[Any]]("Filter", FILTER,
    "dialect" -> enum(ScriptDialect) default ScriptDialect.MVEL, "predicate" -> TEXTAREA) {
    def createBlock(json: JsonObject) = ScriptFilter[Any]
    def setupAttributes(block: ScriptFilter[Any], json: JsonObject, blocks: DSABlockMap) = {
      set(block.dialect, json, "dialect")
      init(block.predicate, json, "predicate", blocks)
    }
  }

  object DebounceAdapter extends TransformerAdapter[Any, Debounce[Any]](
    "Debounce", FILTER, "timeout" -> NUMBER default 500) {
    def createBlock(json: JsonObject) = Debounce[Any]
    def setupAttributes(block: Debounce[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.timeout, json, "timeout", blocks)
    }
  }

  object DistinctAdapter extends TransformerAdapter[Any, Distinct[Any]](
    "Distinct", FILTER, "global" -> BOOLEAN default true) {
    def createBlock(json: JsonObject) = Distinct[Any]
    def setupAttributes(block: Distinct[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.global, json, "global", blocks)
      block.selector <~ (identity[Any] _)
    }
  }

  /* combine */

  object CombineLatestAdapter extends AbstractRxBlockAdapter[CombineLatest[Any]](
    "CombineLatest", COMBINE, "input" -> listOf(TABLE)) {
    def createBlock(json: JsonObject) = CombineLatest[Any]
    def setupBlock(block: CombineLatest[Any], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.sources, json, arrayField, blocks)
    }
  }

  object ConcatAdapter extends AbstractRxBlockAdapter[Concat[Any]]("Concat", COMBINE, input(1), input(2)) {
    def createBlock(json: JsonObject) = Concat[Any]
    def setupBlock(block: Concat[Any], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.source1, json, "input1", blocks)
      connect(block.source2, json, "input2", blocks)
    }
  }

  object MergeAdapter extends AbstractRxBlockAdapter[Merge[Any]]("Merge", COMBINE, input(1), input(2)) {
    def createBlock(json: JsonObject) = Merge[Any]
    def setupBlock(block: Merge[Any], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.source1, json, "input1", blocks)
      connect(block.source2, json, "input2", blocks)
    }
  }

  object ZipAdapter extends AbstractRxBlockAdapter[Zip[Any]]("Zip", COMBINE, "input" -> listOf(TABLE)) {
    def createBlock(json: JsonObject) = Zip[Any]
    def setupBlock(block: Zip[Any], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.sources, json, arrayField, blocks)
    }
  }

  object SelectFirstAdapter extends AbstractRxBlockAdapter[AMB[Any]]("SelectFirst", COMBINE, "input" -> listOf(TABLE)) {
    def createBlock(json: JsonObject) = AMB[Any]
    def setupBlock(block: AMB[Any], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.sources, json, arrayField, blocks)
    }
  }

  /* aggregate */

  object SumAdapter extends TransformerAdapter[Value, Sum[Value]]("Sum", AGGREGATE) {
    def createBlock(json: JsonObject) = Sum[Value](true)
    def setupAttributes(block: Sum[Value], json: JsonObject, blocks: DSABlockMap) = {}
  }

  object ProductAdapter extends TransformerAdapter[Value, Mul[Value]]("Product", AGGREGATE) {
    def createBlock(json: JsonObject) = Mul[Value](true)
    def setupAttributes(block: Mul[Value], json: JsonObject, blocks: DSABlockMap) = {}
  }

  object MinAdapter extends TransformerAdapter[Value, Min[Value]]("Min", AGGREGATE) {
    def createBlock(json: JsonObject) = Min[Value](true)
    def setupAttributes(block: Min[Value], json: JsonObject, blocks: DSABlockMap) = {}
  }

  object MaxAdapter extends TransformerAdapter[Value, Max[Value]]("Max", AGGREGATE) {
    def createBlock(json: JsonObject) = Max[Value](true)
    def setupAttributes(block: Max[Value], json: JsonObject, blocks: DSABlockMap) = {}
  }

  object AvgAdapter extends TransformerAdapter[Value, Avg[Value]]("Avg", AGGREGATE) {
    def createBlock(json: JsonObject) = Avg[Value](true)
    def setupAttributes(block: Avg[Value], json: JsonObject, blocks: DSABlockMap) = {}
  }

  object BasicStatsAdapter extends TransformerAdapter[Value, BasicStats[Value]]("Stats", AGGREGATE) {
    def createBlock(json: JsonObject) = BasicStats[Value](true)
    def setupAttributes(block: BasicStats[Value], json: JsonObject, blocks: DSABlockMap) = {}
  }

  object CountAdapter extends TransformerAdapter[Any, ScriptCount[Any]]("Count", AGGREGATE,
    "dialect" -> enum(ScriptDialect) default ScriptDialect.MVEL, "predicate" -> TEXTAREA) {
    def createBlock(json: JsonObject) = ScriptCount[Any](true)
    def setupAttributes(block: ScriptCount[Any], json: JsonObject, blocks: DSABlockMap) = {
      set(block.dialect, json, "dialect")
      init(block.predicate, json, "predicate", blocks)
    }
  }
}