package org.dsa.iot.ignition

import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.Milliseconds
import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.dslink.util.json.{ JsonArray, JsonObject }
import org.json4s.{ jvalue2monadic, string2JsonInput }
import org.json4s.jackson.JsonMethods.parse
import com.ignition.{ ConnectionSource, ConnectionTarget, FlowRuntime, MultiInputStep, MultiOutputStep, SingleInputStep, SingleOutputStep, Step, SubFlow, SubFlowFactory, frame }
import com.ignition.frame.{ DataGrid, FrameSubFlow, SparkRuntime }
import com.ignition.stream
import com.ignition.stream.{ DataStream, SparkStreamingRuntime, StreamSubFlow, StreamStep }
import com.ignition.types.TypeUtils
import com.ignition.util.JsonUtils.RichJValue
import org.dsa.iot.ignition.step._
import org.json4s.{ jvalue2monadic, string2JsonInput }
import org.json4s.JObject
import org.json4s.jackson.JsonMethods.parse
import com.ignition.frame.BasicAggregator.BasicAggregator

/**
 * Block factory for Stream flows.
 */
object StreamBlockFactory extends BlockFactory[StreamStep, DataStream, SparkStreamingRuntime] {
  type StreamStepAdapter = AbstractStepAdapter[StreamStep, DataStream, SparkStreamingRuntime]

  val flowFactory = StreamSubFlow

  /* native steps */

  object FilterAdapter extends StreamStepAdapter("Filter", One, List("True", "False"), "condition" -> "string") {
    def makeStep(json: JsonObject) = stream.Filter(json.get[String]("condition"))
  }

  object WindowAdapter extends StreamStepAdapter("Window", One, One, "windowSize" -> "number", "slideSize" -> "number") {
    def makeStep(json: JsonObject) = {
      val wSize = Milliseconds(json.get[Int]("windowSize"))
      val sSize = Milliseconds(json.get[Int]("slideSize"))
      stream.Window(wSize, sSize)
    }
  }

  object DSAInputAdapter extends StreamStepAdapter("DSAInput", Nil, One, "paths" -> "textarea") {
    def makeStep(json: JsonObject) = {
      val paths = (parse(json.get[String]("paths")) asArray) map { node =>
        val path = node \ "path" asString
        val dataType = TypeUtils.typeForName(node \ "type" asString)
        path -> dataType
      }
      DSAStreamInput(paths)
    }
  }

  object DSAOutputAdapter extends StreamStepAdapter("DSAOutput", One, Nil, "fields" -> "textarea") {
    def makeStep(json: JsonObject) = {
      val fields = (parse(json.get[String]("fields")) asArray) map { node =>
        val name = node \ "name" asString
        val path = node \ "path" asString

        name -> path
      }
      DSAStreamOutput(fields)
    }
  }

  /* foreach wrappers */

  object BasicStatsAdapter extends StreamStepAdapter("BasicStats", One, One, "fields" -> "textarea", "groupBy" -> "string") {
    def makeStep(json: JsonObject) = {
      val dataFields = (parse(json.get[String]("fields")) asArray) map {
        case JObject(field :: Nil) =>
          val name = field._1
          val func = field._2 asString

          name -> (frame.BasicAggregator.withName(func): BasicAggregator)
        case _ => throw new IllegalArgumentException("Invalid format: " + json)
      }
      val groupFields = (parse(json.get[String]("groupBy")) asArray) map (_ asString)
      stream.foreach(frame.BasicStats(dataFields, groupFields))
    }
  }

  object ColumnStatsAdapter extends StreamStepAdapter("ColumnStats", One, One, "fields" -> "string", "groupBy" -> "string") {
    def makeStep(json: JsonObject) = {
      val dataFields = (parse(json.get[String]("fields")) asArray) map (_ asString)
      val groupFields = (parse(json.get[String]("groupBy")) asArray) map (_ asString)
      stream.foreach(frame.mllib.ColumnStats(dataFields, groupFields))
    }
  }

  object DebugAdapter extends StreamStepAdapter("Debug", One, One) {
    def makeStep(json: JsonObject) = stream.foreach(frame.DebugOutput())
  }

  val adapters = List(
    BasicStatsAdapter,
    ColumnStatsAdapter,
    FilterAdapter,
    WindowAdapter,
    DSAInputAdapter,
    DSAOutputAdapter,
    DebugAdapter)
}