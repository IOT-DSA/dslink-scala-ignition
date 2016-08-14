package org.dsa.iot.spark

import org.apache.spark.streaming.Milliseconds
import org.dsa.iot.dslink.util.json.JsonObject

import com.ignition.{ frame, stream }
import com.ignition.stream.{ DataStream, SparkStreamingRuntime, StreamStep, StreamSubFlow }
import com.ignition.types.TypeUtils

/**
 * Block factory for Stream flows.
 */
object StreamBlockFactory extends BlockFactory[StreamStep, DataStream, SparkStreamingRuntime] {
  type StreamStepAdapter = AbstractStepAdapter[StreamStep, DataStream, SparkStreamingRuntime]

  val flowFactory = StreamSubFlow

  object Categories {
    val STATS = "Statistics"
    val INPUT = "Input"
    val OUTPUT = "Output"
    val TRANSFORM = "Transform"
    val FLOW = "Flow"
    val UTIL = "Utility"
    val SCRIPT = "Scripting"
    val DSA = "DSA"
  }
  import Categories._

  /* native steps */

  /**
   * Filter.
   */
  object FilterAdapter extends StreamStepAdapter("Filter", FLOW, One, List("True", "False"), "condition" -> TEXT) {
    def makeStep(json: JsonObject) = stream.Filter(json asString "condition")
  }

  /**
   * Join.
   */
  object JoinAdapter extends StreamStepAdapter("Join", FLOW, Two, One,
    "condition" -> TEXT, "joinType" -> enum(frame.JoinType)) {
    def makeStep(json: JsonObject) = stream.Join(
      json asString "condition",
      json.asEnum[frame.JoinType.JoinType](frame.JoinType)("joinType"))
  }

  /**
   * Set Variables.
   */
  object SetVariablesAdapter extends StreamStepAdapter("SetVariables", UTIL, One, One,
    "name 0" -> TEXT, "type 0" -> DATA_TYPE, "value 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val fields = json.asTupledList3[String, String, String]("@array") map {
        case (name, typeName, strValue) => name -> parseValue(strValue, noneIfEmpty(typeName))
      }
      stream.SetVariables(fields.toMap)
    }
  }

  /**
   * Window.
   */
  object WindowAdapter extends StreamStepAdapter("Window", FLOW, One, One, "windowSize" -> "number", "slideSize" -> "number") {
    def makeStep(json: JsonObject) = {
      val wSize = Milliseconds(json asInt "windowSize")
      val sSize = Milliseconds(json asInt "slideSize")
      stream.Window(wSize, sSize)
    }
  }

  /**
   * Kafka Input.
   */
  object KafkaInputAdapter extends StreamStepAdapter("KafkaInput", INPUT, Nil, One,
    "brokers" -> TEXT, "topics" -> TEXT,
    "propName 0" -> TEXT, "propValue 0" -> TEXT, "fieldName" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val brokers = splitAndTrim(",")(json asString "brokers")
      val topics = splitAndTrim(",")(json asString "topics")
      val props = json.asTupledList2[String, String]("@array") map {
        case (name, value) => name -> value
      }
      val fieldName = json asString "fieldName"
      stream.KafkaInput(brokers, topics, props.toMap, fieldName)
    }
  }

  /**
   * DSA Input.
   */
  object DSAInputAdapter extends StreamStepAdapter("DSAInput", DSA, Nil, One, "path 0" -> TEXT, "type 0" -> DATA_TYPE) {
    def makeStep(json: JsonObject) = {
      val paths = json.asTupledList2[String, String]("@array") map {
        case (path, typeName) => path -> TypeUtils.typeForName(typeName)
      }
      step.DSAStreamInput(paths)
    }
  }

  /**
   * DSA Output.
   */
  object DSAOutputAdapter extends StreamStepAdapter("DSAOutput", DSA, One, Nil, "field 0" -> TEXT, "path 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val fields = json.asTupledList2[String, String]("@array") map {
        case (name, path) => name -> path
      }
      step.DSAStreamOutput(fields)
    }
  }

  /* foreach wrappers */

  /**
   * Add Fields.
   */
  object AddFieldsAdapter extends StreamStepAdapter("AddFields", TRANSFORM, One, One,
    "name 0" -> TEXT, "type 0" -> DATA_TYPE, "value 0" -> TEXT) {
    def makeStep(json: JsonObject) = stream.foreach(FrameBlockFactory.AddFieldsAdapter.makeStep(json))
  }

  /**
   * Basic Stats.
   */
  object BasicStatsAdapter extends StreamStepAdapter("BasicStats", STATS, One, One,
    "field 0" -> TEXT, "func 0" -> enum(frame.BasicAggregator), "groupBy" -> TEXT) {
    def makeStep(json: JsonObject) = stream.foreach(FrameBlockFactory.BasicStatsAdapter.makeStep(json))
  }

  /**
   * Debug.
   */
  object DebugAdapter extends StreamStepAdapter("Debug", UTIL, One, One) {
    def makeStep(json: JsonObject) = stream.foreach(FrameBlockFactory.DebugAdapter.makeStep(json))
  }

  /**
   * Kafka Output.
   */
  object KafkaOutputAdapter extends StreamStepAdapter("KafkaOutput", OUTPUT, One, One, "field" -> TEXT,
    "topic" -> TEXT, "brokers" -> TEXT, "propName 0" -> TEXT, "propValue 0" -> TEXT) {
    def makeStep(json: JsonObject) = stream.foreach(FrameBlockFactory.KafkaOutputAdapter.makeStep(json))
  }

  /**
   * Formula.
   */
  object FormulaAdapter extends StreamStepAdapter("Formula", SCRIPT, One, One,
    "name 0" -> TEXT, "dialect 0" -> enum("mvel", "xml", "json"), "expression 0" -> TEXT, "source 0" -> TEXT) {
    def makeStep(json: JsonObject) = stream.foreach(FrameBlockFactory.FormulaAdapter.makeStep(json))
  }

  /**
   * Reduce.
   */
  object ReduceAdapter extends StreamStepAdapter("Reduce", TRANSFORM, One, One,
    "name 0" -> TEXT, "operation 0" -> TEXT, "groupBy" -> TEXT) {
    def makeStep(json: JsonObject) = stream.foreach(FrameBlockFactory.ReduceAdapter.makeStep(json))
  }

  /**
   * Select Values.
   */
  object SelectValuesAdapter extends StreamStepAdapter("SelectValues", TRANSFORM, One, One,
    "action 0" -> enum("retain", "rename", "remove", "retype"), "data 0" -> TEXT) {
    def makeStep(json: JsonObject) = stream.foreach(FrameBlockFactory.SelectValuesAdapter.makeStep(json))
  }

  /**
   * SQL Query.
   */
  object SQLQueryAdapter extends StreamStepAdapter("SQLQuery", SCRIPT, Two, One, "query" -> TEXTAREA) {
    def makeStep(json: JsonObject) = stream.foreach(FrameBlockFactory.SQLQueryAdapter.makeStep(json))
  }

  /**
   * Column Stats.
   */
  object ColumnStatsAdapter extends StreamStepAdapter("ColumnStats", STATS, One, One,
    "field 0" -> TEXT, "groupBy" -> TEXT) {
    def makeStep(json: JsonObject) = stream.foreach(FrameBlockFactory.ColumnStatsAdapter.makeStep(json))
  }

  /**
   * Correlation.
   */
  object CorrelationAdapter extends StreamStepAdapter("Correlation", STATS, One, One,
    "field 0" -> TEXT, "groupBy" -> TEXT, "method" -> enum(frame.mllib.CorrelationMethod)) {
    def makeStep(json: JsonObject) = stream.foreach(FrameBlockFactory.CorrelationAdapter.makeStep(json))
  }

  /**
   * Regression.
   */
  object RegressionAdapter extends StreamStepAdapter("Regression", STATS, One, One,
    "labelField" -> TEXT, "field 0" -> TEXT, "groupBy" -> TEXT,
    "method" -> enum(frame.mllib.RegressionMethod),
    "iterations" -> NUMBER, "step" -> NUMBER, "intercept" -> BOOLEAN) {
    def makeStep(json: JsonObject) = stream.foreach(FrameBlockFactory.RegressionAdapter.makeStep(json))
  }

  /**
   * List of available adapters retrieved through reflection.
   */
  val adapters = buildAdapterList[StreamBlockFactory.type]
}