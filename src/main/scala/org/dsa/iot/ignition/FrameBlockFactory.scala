package org.dsa.iot.ignition

import org.apache.spark.sql.DataFrame
import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.ignition.step.{ DSAInput, DSAOutput }
import org.json4s.{ jvalue2monadic, string2JsonInput }
import org.json4s.JObject
import org.json4s.jackson.JsonMethods.parse
import com.ignition.frame
import com.ignition.frame.{ EnvLiteral, FrameSubFlow, SparkRuntime, VarLiteral, FrameStep }
import com.ignition.frame.BasicAggregator.{ BasicAggregator, valueToAggregator }
import com.ignition.types.TypeUtils
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.frame.mllib.CorrelationMethod

/**
 * Block factory for Frame flows.
 */
object FrameBlockFactory extends BlockFactory[FrameStep, DataFrame, SparkRuntime] {
  type FrameStepAdapter = AbstractStepAdapter[FrameStep, DataFrame, SparkRuntime]

  val flowFactory = FrameSubFlow

  /**
   * Add Fields.
   */
  object AddFieldsAdapter extends FrameStepAdapter("AddFields", One, One, "fields" -> "textarea") {
    def makeStep(json: JsonObject) = {
      val fields = (parse(json.get[String]("fields")) asArray) map { node =>
        val name = node \ "name" asString
        val typeStr = node \ "type" asString
        val jValue = node \ "value"
        val value = typeStr match {
          case "env" => EnvLiteral(jValue asString)
          case "var" => VarLiteral(jValue asString)
          case t @ _ => TypeUtils.jsonToValue(TypeUtils.typeForName(t), jValue)
        }
        name -> value
      }
      frame.AddFields(fields)
    }
  }

  /**
   * Basic Stats.
   */
  object BasicStatsAdapter extends FrameStepAdapter("BasicStats", One, One, "fields" -> "textarea", "groupBy" -> "string") {
    def makeStep(json: JsonObject) = {
      val dataFields = (parse(json.get[String]("fields")) asArray) map {
        case JObject(field :: Nil) =>
          val name = field._1
          val func = field._2 asString

          name -> (frame.BasicAggregator.withName(func): BasicAggregator)
        case _ => throw new IllegalArgumentException("Invalid format: " + json)
      }
      val groupFields = (parse(json.get[String]("groupBy")) asArray) map (_ asString)
      frame.BasicStats(dataFields, groupFields)
    }
  }
  
  /**
   * Column Stats.
   */
  object ColumnStatsAdapter extends FrameStepAdapter("ColumnStats", One, One, "fields" -> "string", "groupBy" -> "string") {
    def makeStep(json: JsonObject) = {
      val dataFields = (parse(json.get[String]("fields")) asArray) map (_ asString)
      val groupFields = (parse(json.get[String]("groupBy")) asArray) map (_ asString)
      frame.mllib.ColumnStats(dataFields, groupFields)
    }
  }

  /**
   * Correlation.
   */
  object CorrelationStatsAdapter extends FrameStepAdapter("Correlation", One, One, "fields" -> "string", 
      "groupBy" -> "string", "method" -> "enum[PEARSON,SPEARMAN]") {
    def makeStep(json: JsonObject) = {
      val dataFields = (parse(json.get[String]("fields")) asArray) map (_ asString)
      val groupFields = (parse(json.get[String]("groupBy")) asArray) map (_ asString)
      val method = CorrelationMethod.withName(json.get[String]("method"))
      frame.mllib.Correlation(dataFields, groupFields, method)
    }
  }
  
  /**
   * Cassandra Input.
   */
  object CassandraInputAdapter extends FrameStepAdapter("CassandraInput", Nil, One, "keyspace" -> "string",
    "table" -> "string", "columns" -> "string", "where" -> "string") {
    def makeStep(json: JsonObject) = {
      val keyspace = json.get[String]("keyspace").trim
      val table = json.get[String]("table").trim
      val columnList = Option(json.get[String]("columns")).map(_.split("\\s*,\\s*").toList).getOrElse(Nil)
      val columns = columnList.filter(_.length > 0)
      val whereStr = json.get[String]("where")
      val where = if (!whereStr.isEmpty) Some(frame.Where(whereStr)) else None
      frame.CassandraInput(keyspace, table, columns, where)
    }
  }

  /**
   * Cassandra Output.
   */
  object CassandraOutputAdapter extends FrameStepAdapter("CassandraOutput", One, Nil,
    "keyspace" -> "string", "table" -> "string") {
    def makeStep(json: JsonObject) = {
      val keyspace = json.get[String]("keyspace").trim
      val table = json.get[String]("table").trim
      frame.CassandraOutput(keyspace, table)
    }
  }

  /**
   * Data Grid.
   */
  object DataGridAdapter extends FrameStepAdapter("DataGrid", Nil, One, "schema" -> "textarea", "rows" -> "textarea") {
    def makeStep(json: JsonObject) = {
      val schema = frame.DataGrid.jsonToSchema(parse(json.get[String]("schema")))
      val rows = (parse(json.get[String]("rows")) asArray) map { jr =>
        val items = schema zip jr.asArray map {
          case (field, value) => TypeUtils.jsonToValue(field.dataType, value)
        }
        org.apache.spark.sql.Row.fromSeq(items)
      }
      frame.DataGrid(schema, rows)
    }
  }

  /**
   * Filter.
   */
  object FilterAdapter extends FrameStepAdapter("Filter", One, List("True", "False"), "condition" -> "string") {
    def makeStep(json: JsonObject) = frame.Filter(json.get[String]("condition"))
  }

  /**
   * DSA Input.
   */
  object DSAInputAdapter extends FrameStepAdapter("DSAInput", Nil, One, "paths" -> "textarea") {
    def makeStep(json: JsonObject) = {
      val paths = (parse(json.get[String]("paths")) asArray) map { node =>
        val path = node \ "path" asString
        val dataType = TypeUtils.typeForName(node \ "type" asString)
        path -> dataType
      }
      DSAInput(paths)
    }
  }

  /**
   * DSA Output.
   */
  object DSAOutputAdapter extends FrameStepAdapter("DSAOutput", One, Nil, "fields" -> "textarea") {
    def makeStep(json: JsonObject) = {
      val fields = (parse(json.get[String]("fields")) asArray) map { node =>
        val name = node \ "name" asString
        val path = node \ "path" asString

        name -> path
      }
      DSAOutput(fields)
    }
  }

  /**
   * Intersection.
   */
  object IntersectionAdapter extends FrameStepAdapter("Intersection", Two, One) {
    def makeStep(json: JsonObject) = frame.Intersection()
  }

  /**
   * Join.
   */
  object JoinAdapter extends FrameStepAdapter("Join", Two, One,
    "condition" -> "string", "joinType" -> "enum[INNER,OUTER,LEFT,RIGHT,SEMI]") {
    def makeStep(json: JsonObject) = frame.Join(
      json.get[String]("condition"),
      frame.JoinType.withName(json.get[String]("joinType")))
  }

  /**
   * SQL Query.
   */
  object SQLQueryAdapter extends FrameStepAdapter("SQLQuery", Two, One, "query" -> "string") {
    def makeStep(json: JsonObject) = frame.SQLQuery(json.get[String]("query"))
  }

  /**
   * Debug.
   */
  object DebugAdapter extends FrameStepAdapter("Debug", One, One) {
    def makeStep(json: JsonObject) = frame.DebugOutput()
  }

  val adapters = List(
    AddFieldsAdapter,
    BasicStatsAdapter,
    CassandraInputAdapter,
    CassandraOutputAdapter,
    ColumnStatsAdapter,
    DataGridAdapter,
    DSAInputAdapter,
    DSAOutputAdapter,
    FilterAdapter,
    IntersectionAdapter,
    JoinAdapter,
    SQLQueryAdapter,
    DebugAdapter)
}