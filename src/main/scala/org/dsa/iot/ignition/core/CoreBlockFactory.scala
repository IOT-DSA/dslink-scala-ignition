package org.dsa.iot.ignition.core

import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.ignition._
import org.dsa.iot.ignition.Main.requester
import org.dsa.iot.ignition.NUMBER
import org.dsa.iot.ignition.ParamInfo.input
import com.ignition.rx.core._
import com.ignition.rx.numeric._

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
    def setupBlock(block: DSAInput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.path, json, "path", blocks)
    }
  }

  object IntervalAdapter extends AbstractRxBlockAdapter[Interval]("Interval", INPUT,
    "initial" -> NUMBER default 0, "period" -> NUMBER default 1000) {
    def createBlock(json: JsonObject) = new Interval
    def setupBlock(block: Interval, json: JsonObject, blocks: DSABlockMap) = {
      init(block.initial, json, "initial", blocks)
      init(block.period, json, "period", blocks)
    }
  }

  object NumericRangeAdapter extends AbstractRxBlockAdapter[NumericRange]("NumericRange", INPUT,
    "start" -> NUMBER default 0, "end" -> NUMBER default 10, "step" -> NUMBER default 1) {
    def createBlock(json: JsonObject) = new NumericRange
    def setupBlock(block: NumericRange, json: JsonObject, blocks: DSABlockMap) = {
      init(block.start, json, "start", blocks)
      init(block.end, json, "end", blocks)
      init(block.step, json, "step", blocks)
    }
  }

  /* transform */

  object TakeByCountAdapter extends AbstractRxBlockAdapter[TakeByCount[Any]]("TakeBySize", TRANSFORM,
    "count" -> NUMBER default 10, input) {
    def createBlock(json: JsonObject) = new TakeByCount[Any]
    def setupBlock(block: TakeByCount[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.count, json, "count", blocks)
      connect(block.source, json, "input", blocks)
    }
  }

  object TakeByTimeAdapter extends AbstractRxBlockAdapter[TakeByTime[Any]]("TakeByTime", TRANSFORM,
    "period" -> NUMBER default 10000, input) {
    def createBlock(json: JsonObject) = new TakeByTime[Any]
    def setupBlock(block: TakeByTime[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.period, json, "period", blocks)
      connect(block.source, json, "input", blocks)
    }
  }

  object TakeRightAdapter extends AbstractRxBlockAdapter[TakeRight[Any]]("TakeRight", TRANSFORM,
    "period" -> NUMBER default 10000, "count" -> NUMBER default 10, input) {
    def createBlock(json: JsonObject) = new TakeRight[Any]
    def setupBlock(block: TakeRight[Any], json: JsonObject, blocks: DSABlockMap) = {
      set(block.period, json, "period")
      set(block.count, json, "count")
      connect(block.source, json, "input", blocks)
    }
  }

  object WindowByTimeAdapter extends AbstractRxBlockAdapter[WindowByTime[Any]]("WindowByTime",
    TRANSFORM, "span" -> NUMBER default 10000, "shift" -> NUMBER default 1000, input) {
    def createBlock(json: JsonObject) = new WindowByTime[Any]
    def setupBlock(block: WindowByTime[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.span, json, "span", blocks)
      init(block.shift, json, "shift", blocks)
      connect(block.source, json, "input", blocks)
    }
  }

  object WindowBySizeAdapter extends AbstractRxBlockAdapter[WindowBySize[Any]]("WindowBySize",
    TRANSFORM, "count" -> NUMBER default 10, "skip" -> NUMBER default 1, input) {
    def createBlock(json: JsonObject) = new WindowBySize[Any]
    def setupBlock(block: WindowBySize[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.count, json, "count", blocks)
      init(block.skip, json, "skip", blocks)
      connect(block.source, json, "input", blocks)
    }
  }

  object EvaluateAdapter extends AbstractRxBlockAdapter[ScriptMap]("Evaluate", TRANSFORM,
    "dialect" -> enum(ScriptDialect) default ScriptDialect.MVEL, "script" -> TEXTAREA, input) {
    def createBlock(json: JsonObject) = new ScriptMap
    def setupBlock(block: ScriptMap, json: JsonObject, blocks: DSABlockMap) = {
      set(block.dialect, json, "dialect")
      init(block.script, json, "script", blocks)
      connect(block.source, json, "input", blocks)
    }
  }

  object ZipWithIndex extends AbstractRxBlockAdapter[ZipWithIndex[Any]]("ZipWithIndex", TRANSFORM, input) {
    def createBlock(json: JsonObject) = new ZipWithIndex[Any]
    def setupBlock(block: ZipWithIndex[Any], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.source, json, "input", blocks)
    }
  }

  /* filter */

  object FilterAdapter extends AbstractRxBlockAdapter[ScriptFilter]("Filter", FILTER,
    "dialect" -> enum(ScriptDialect) default ScriptDialect.MVEL, "predicate" -> TEXTAREA, input) {
    def createBlock(json: JsonObject) = new ScriptFilter
    def setupBlock(block: ScriptFilter, json: JsonObject, blocks: DSABlockMap) = {
      set(block.dialect, json, "dialect")
      init(block.predicate, json, "predicate", blocks)
      connect(block.source, json, "input", blocks)
    }
  }

  object DebounceAdapter extends AbstractRxBlockAdapter[Debounce[Any]](
    "Debounce", FILTER, "timeout" -> NUMBER default 500, input) {
    def createBlock(json: JsonObject) = new Debounce[Any]
    def setupBlock(block: Debounce[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.timeout, json, "timeout", blocks)
      connect(block.source, json, "input", blocks)
    }
  }

  object DistinctAdapter extends AbstractRxBlockAdapter[Distinct[Any]](
    "Distinct", FILTER, "global" -> BOOLEAN default true, input) {
    def createBlock(json: JsonObject) = {
      val block = new Distinct[Any]
      block.selector <~ (identity[Any] _)
      block
    }
    def setupBlock(block: Distinct[Any], json: JsonObject, blocks: DSABlockMap) = {
      init(block.global, json, "global", blocks)
      connect(block.source, json, "input", blocks)
    }
  }

  /* combine */

  object CombineLatestAdapter extends AbstractRxBlockAdapter[CombineLatest[Any]](
    "CombineLatest", COMBINE, "input 0" -> TABLE) {
    def createBlock(json: JsonObject) = new CombineLatest[Any]
    def setupBlock(block: CombineLatest[Any], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.sources, json, "@array", blocks)
    }
  }

  object ConcatAdapter extends AbstractRxBlockAdapter[Concat[Any]](
    "Concat", COMBINE, input(1), input(2)) {
    def createBlock(json: JsonObject) = new Concat[Any]
    def setupBlock(block: Concat[Any], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.source1, json, "input1", blocks)
      connect(block.source2, json, "input2", blocks)
    }
  }

  object MergeAdapter extends AbstractRxBlockAdapter[Merge[Any]](
    "Merge", COMBINE, input(1), input(2)) {
    def createBlock(json: JsonObject) = new Merge[Any]
    def setupBlock(block: Merge[Any], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.source1, json, "input1", blocks)
      connect(block.source2, json, "input2", blocks)
    }
  }

  object ZipAdapter extends AbstractRxBlockAdapter[Zip[Any]](
    "Zip", COMBINE, "input 0" -> TABLE) {
    def createBlock(json: JsonObject) = new Zip[Any]
    def setupBlock(block: Zip[Any], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.sources, json, "@array", blocks)
    }
  }

  /* aggregate */

  object SumAdapter extends AbstractRxBlockAdapter[Sum[RichValue]](
    "Sum", AGGREGATE, input) {
    def createBlock(json: JsonObject) = new Sum[RichValue]
    def setupBlock(block: Sum[RichValue], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.source, json, "input", blocks)
    }
  }

  object ProductAdapter extends AbstractRxBlockAdapter[Mul[RichValue]](
    "Product", AGGREGATE, input) {
    def createBlock(json: JsonObject) = new Mul[RichValue]
    def setupBlock(block: Mul[RichValue], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.source, json, "input", blocks)
    }
  }

  object MinAdapter extends AbstractRxBlockAdapter[Min[RichValue]](
    "Min", AGGREGATE, input) {
    def createBlock(json: JsonObject) = new Min[RichValue]
    def setupBlock(block: Min[RichValue], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.source, json, "input", blocks)
    }
  }

  object MaxAdapter extends AbstractRxBlockAdapter[Max[RichValue]](
    "Max", AGGREGATE, input) {
    def createBlock(json: JsonObject) = new Max[RichValue]
    def setupBlock(block: Max[RichValue], json: JsonObject, blocks: DSABlockMap) = {
      connect(block.source, json, "input", blocks)
    }
  }
}