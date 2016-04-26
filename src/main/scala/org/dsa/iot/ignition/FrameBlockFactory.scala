package org.dsa.iot.ignition

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ StructField, StructType }
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.dsa.iot.dslink.util.json.JsonObject
import com.ignition.frame
import com.ignition.frame.{ FrameStep, FrameSubFlow }
import com.ignition.frame.BasicAggregator.{ BasicAggregator, valueToAggregator }
import com.ignition.frame.HttpMethod.HttpMethod
import com.ignition.frame.JoinType.JoinType
import com.ignition.frame.ReduceOp.{ ReduceOp, valueToOp }
import com.ignition.frame.SparkRuntime
import com.ignition.frame.mllib.CorrelationMethod.CorrelationMethod
import com.ignition.frame.mllib.RegressionMethod.RegressionMethod
import com.ignition.script.{ JsonPathExpression, MvelExpression, XPathExpression }
import com.ignition.types.TypeUtils
import org.apache.spark.sql.SaveMode

/**
 * Block factory for Frame flows.
 */
object FrameBlockFactory extends BlockFactory[FrameStep, DataFrame, SparkRuntime] {
  type FrameStepAdapter = AbstractStepAdapter[FrameStep, DataFrame, SparkRuntime]
  import TypeUtils._

  val flowFactory = FrameSubFlow

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

  /**
   * Add Fields.
   */
  object AddFieldsAdapter extends FrameStepAdapter("AddFields", TRANSFORM, One, One,
    "name 0" -> TEXT, "type 0" -> DATA_TYPE, "value 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val fields = json.asTupledList3[String, String, String]("@array") map {
        case (name, typeName, strValue) => name -> parseValue(strValue, noneIfEmpty(typeName))
      }
      frame.AddFields(fields.toList)
    }
  }

  /**
   * Basic Stats.
   */
  object BasicStatsAdapter extends FrameStepAdapter("BasicStats", STATS, One, One,
    "field 0" -> TEXT, "func 0" -> enum(frame.BasicAggregator), "groupBy" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val dataFields = json.asTupledList2[String, String]("@array") map {
        case (name, strFunc) => name -> (frame.BasicAggregator.withName(strFunc): BasicAggregator)
      }
      val groupFields = json getAsString "groupBy" map splitAndTrim(",") getOrElse Nil
      frame.BasicStats(dataFields.toList, groupFields)
    }
  }

  /**
   * Cassandra Input.
   */
  object CassandraInputAdapter extends FrameStepAdapter("CassandraInput", INPUT, Nil, One, "keyspace" -> TEXT,
    "table" -> TEXT, "columns" -> TEXT, "where" -> TEXTAREA) {
    def makeStep(json: JsonObject) = {
      val keyspace = json asString "keyspace"
      val table = json asString "table"
      val columns = json getAsString "columns" map splitAndTrim(",") getOrElse Nil
      val where = json getAsString "where" map (frame.Where(_))
      frame.CassandraInput(keyspace, table, columns, where)
    }
  }

  /**
   * Cassandra Output.
   */
  object CassandraOutputAdapter extends FrameStepAdapter("CassandraOutput", OUTPUT, One, Nil,
    "keyspace" -> TEXT, "table" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val keyspace = json asString "keyspace"
      val table = json asString "table"
      frame.CassandraOutput(keyspace, table)
    }
  }

  /**
   * Csv File Input.
   */
  object CsvFileInputAdapter extends FrameStepAdapter("CsvFileInput", INPUT, Nil, One,
    "path" -> TEXT, "separator" -> TEXT,
    "name 0" -> TEXT, "type 0" -> DATA_TYPE, "nullable 0" -> BOOLEAN) {
    def makeStep(json: JsonObject) = {
      val path = json asString "path"
      val separator = json getAsString "separator"
      val fields = json.asTupledList3[String, String, String]("@array") map {
        case (name, typeName, nullable) => new StructField(name, typeForName(typeName), nullable.toBoolean)
      }
      val schema = if (fields.isEmpty) None else Some(StructType(fields))
      frame.CsvFileInput(path, separator, schema)
    }
  }

  /**
   * Data Grid.
   */
  object DataGridAdapter extends FrameStepAdapter("DataGrid", INPUT, Nil, One, "schema" -> TEXT, "row 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val fields = splitAndTrim(",")(json asString "schema") map { s =>
        val Array(name, typeName) = s.split(":")
        new StructField(name, typeForName(typeName), true)
      }
      val schema = StructType(fields)
      val rows = json asStringList "@array" map { s =>
        val parts = s.split(",").map(_.trim)
        val items = schema zip parts map {
          case (field, value) => valueOf(value, field.dataType)
        }
        org.apache.spark.sql.Row.fromSeq(items)
      }
      frame.DataGrid(schema, rows)
    }
  }

  /**
   * Debug.
   */
  object DebugAdapter extends FrameStepAdapter("Debug", UTIL, One, One) {
    def makeStep(json: JsonObject) = frame.DebugOutput()
  }

  /**
   * Filter.
   */
  object FilterAdapter extends FrameStepAdapter("Filter", FLOW, One, List("True", "False"), "condition" -> TEXT) {
    def makeStep(json: JsonObject) = frame.Filter(json asString "condition")
  }

  /**
   * Formula.
   */
  object FormulaAdapter extends FrameStepAdapter("Formula", SCRIPT, One, One,
    "name 0" -> TEXT, "dialect 0" -> enum("mvel", "xml", "json"), "expression 0" -> TEXT, "source 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val fields = json.asTupledList4[String, String, String, String]("@array") map {
        case (name, "mvel", expression, src) => name -> MvelExpression(expression)
        case (name, "xml", expression, src)  => name -> XPathExpression(expression, src)
        case (name, "json", expression, src) => name -> JsonPathExpression(expression, src)
      }
      frame.Formula(fields)
    }
  }

  /**
   * Intersection.
   */
  object IntersectionAdapter extends FrameStepAdapter("Intersection", FLOW, Two, One) {
    def makeStep(json: JsonObject) = frame.Intersection()
  }

  /**
   * JDBC Input.
   */
  object JdbcInputAdapter extends FrameStepAdapter("JdbcInput", INPUT, Nil, One,
    "url" -> TEXT, "username" -> TEXT, "password" -> TEXT, "sql" -> TEXTAREA,
    "propName 0" -> TEXT, "propValue 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val url = json asString "url"
      val username = json getAsString "username"
      val password = json getAsString "password"
      val sql = json asString "sql"
      val props = json.asTupledList2[String, String]("@array") map {
        case (name, value) => name -> value
      }
      frame.JdbcInput(url, sql, username, password, props.toMap)
    }
  }

  /**
   * JDBC Output.
   */
  object JdbcOutputAdapter extends FrameStepAdapter("JdbcOutput", OUTPUT, One, One,
    "url" -> TEXT, "username" -> TEXT, "password" -> TEXT, "table" -> TEXT,
    "mode" -> enum(SaveMode.values.map(_.name): _*),
    "propName 0" -> TEXT, "propValue 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val url = json asString "url"
      val username = json getAsString "username"
      val password = json getAsString "password"
      val table = json asString "table"
      val mode = SaveMode.valueOf(json asString "mode")
      val props = json.asTupledList2[String, String]("@array") map {
        case (name, value) => name -> value
      }
      frame.JdbcOutput(url, table, mode, username, password, props.toMap)
    }
  }

  /**
   * Join.
   */
  object JoinAdapter extends FrameStepAdapter("Join", FLOW, Two, One,
    "condition" -> TEXT, "joinType" -> enum(frame.JoinType)) {
    def makeStep(json: JsonObject) = frame.Join(
      json asString "condition",
      json.asEnum[JoinType](frame.JoinType)("joinType"))
  }

  /**
   * JSON File Input.
   */
  object JsonFileInputAdapter extends FrameStepAdapter("JsonFileInput", INPUT, Nil, One,
    "path" -> TEXT, "name 0" -> TEXT, "jsonPath 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val path = json asString "path"
      val columns = json.asTupledList2[String, String]("@array") map {
        case (name, jsonPath) => name -> jsonPath
      }
      frame.JsonFileInput(path, columns)
    }
  }

  /**
   * Kafka Input.
   */
  object KafkaInputAdapter extends FrameStepAdapter("KafkaInput", INPUT, Nil, One, "zkUrl" -> TEXT,
    "topic" -> TEXT, "groupId" -> TEXT, "maxRows" -> NUMBER, "maxTimeout" -> NUMBER,
    "propName 0" -> TEXT, "propValue 0" -> TEXT, "fieldName" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val zkUrl = json asString "zkUrl"
      val topic = json asString "topic"
      val groupId = json asString "groupId"
      val maxRows = json getAsInt "maxRows"
      val maxTimeout = json getAsLong "maxTimeout"
      val props = json.asTupledList2[String, String]("@array") map {
        case (name, value) => name -> value
      }
      val fieldName = json asString "fieldName"
      frame.KafkaInput(zkUrl, topic, groupId, maxRows, maxTimeout, props.toMap, fieldName)
    }
  }

  /**
   * Kafka Output.
   */
  object KafkaOutputAdapter extends FrameStepAdapter("KafkaOutput", OUTPUT, One, One, "field" -> TEXT,
    "topic" -> TEXT, "brokers" -> TEXT, "propName 0" -> TEXT, "propValue 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val field = json asString "field"
      val topic = json asString "topic"
      val brokers = splitAndTrim(",")(json asString "brokers")
      val props = json.asTupledList2[String, String]("@array") map {
        case (name, value) => name -> value
      }
      frame.KafkaOutput(field, topic, brokers, props.toMap)
    }
  }

  /**
   * Mongo Input.
   */
  object MongoInputAdapter extends FrameStepAdapter("MongoInput", INPUT, Nil, One, "database" -> TEXT,
    "collection" -> TEXT,
    "name 0" -> TEXT, "type 0" -> DATA_TYPE, "nullable 0" -> BOOLEAN,
    "sort" -> TEXT, "limit" -> NUMBER, "offset" -> NUMBER) {
    def makeStep(json: JsonObject) = {
      val db = json asString "database"
      val coll = json asString "collection"
      val fields = json.asTupledList3[String, String, String]("@array") map {
        case (name, typeName, nullable) => new StructField(name, typeForName(typeName), nullable.toBoolean)
      }
      val schema = StructType(fields)
      val sort = json getAsString "sort" map splitAndTrim(",") getOrElse Nil map (str =>
        str split "\\s+" match {
          case Array(name)      => frame.SortOrder(name)
          case Array(name, dir) => frame.SortOrder(name, dir.toLowerCase startsWith "asc")
        })

      frame.MongoInput(db, coll, schema, Map.empty, sort)
    }
  }

  /**
   * Mongo Output.
   */
  object MongoOutputAdapter extends FrameStepAdapter("MongoOutput", OUTPUT, One, One,
    "database" -> TEXT, "collection" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val db = json asString "database"
      val coll = json asString "collection"
      frame.MongoOutput(db, coll)
    }
  }

  /**
   * Range Input.
   */
  object RangeInputAdapter extends FrameStepAdapter("RangeInput", INPUT, One, One,
    "start" -> NUMBER, "end" -> NUMBER, "step" -> NUMBER) {
    def makeStep(json: JsonObject) = {
      val start = json asLong "start"
      val end = json asLong "end"
      val inc = json asLong "step"
      frame.RangeInput(start, end, inc)
    }
  }

  /**
   * Reduce.
   */
  object ReduceAdapter extends FrameStepAdapter("Reduce", TRANSFORM, One, One,
    "name 0" -> TEXT, "operation 0" -> TEXT, "groupBy" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val reducers = json.asTupledList2[String, String]("@array") map {
        case (name, op) => name -> (frame.ReduceOp.withName(op): ReduceOp)
      }
      val groupFields = json getAsString "groupBy" map splitAndTrim(",") getOrElse Nil
      frame.Reduce(reducers, groupFields)
    }
  }

  /**
   * REST Client.
   */
  object RestClientAdapter extends FrameStepAdapter("RestClient", UTIL, One, One, "url" -> TEXT,
    "method" -> enum(frame.HttpMethod), "body" -> TEXTAREA, "header 0" -> TEXT, "headerValue 0" -> TEXT,
    "resultField" -> TEXT, "statusField" -> TEXT, "headersField" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val url = json asString "url"
      val method = json.asEnum[HttpMethod](frame.HttpMethod)("method")
      val body = json getAsString "body"
      val headers = json.asTupledList2[String, String]("@array") map {
        case (name, value) => name -> value
      }
      val resultField = json getAsString "resultField"
      val statusField = json getAsString "statusField"
      val headersField = json getAsString "headersFeld"
      frame.RestClient(url, method, body, headers.toMap, resultField, statusField, headersField)
    }
  }

  /**
   * Select Values.
   */
  object SelectValuesAdapter extends FrameStepAdapter("SelectValues", TRANSFORM, One, One,
    "action 0" -> enum("retain", "rename", "remove", "retype"), "data 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val actions = json.asTupledList2[String, String]("@array") map {
        case ("retain", data) => frame.SelectAction.Retain(splitAndTrim(",")(data))
        case ("rename", data) =>
          val pairs = splitAndTrim(",")(data) map { str =>
            val Array(oldName, newName) = str.split(":").map(_.trim)
            oldName -> newName
          }
          frame.SelectAction.Rename(pairs.toMap)
        case ("remove", data) => frame.SelectAction.Remove(splitAndTrim(",")(data))
        case ("retype", data) =>
          val pairs = splitAndTrim(",")(data) map { str =>
            val Array(field, typeName) = str.split(":").map(_.trim)
            field -> typeForName(typeName)
          }
          frame.SelectAction.Retype(pairs.toMap)
      }
      frame.SelectValues(actions)
    }
  }

  /**
   * Set Variables.
   */
  object SetVariablesAdapter extends FrameStepAdapter("SetVariables", UTIL, One, One,
    "name 0" -> TEXT, "type 0" -> DATA_TYPE, "value 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val fields = json.asTupledList3[String, String, String]("@array") map {
        case (name, typeName, strValue) => name -> parseValue(strValue, noneIfEmpty(typeName))
      }
      frame.SetVariables(fields.toMap)
    }
  }

  /**
   * SQL Query.
   */
  object SQLQueryAdapter extends FrameStepAdapter("SQLQuery", SCRIPT, Two, One, "query" -> TEXTAREA) {
    def makeStep(json: JsonObject) = frame.SQLQuery(json.get[String]("query"))
  }

  /**
   * Text File Input.
   */
  object TextFileInputAdapter extends FrameStepAdapter("TextFileInput", INPUT, Nil, One,
    "path" -> TEXT, "separator" -> TEXT, "field" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val path = json asString "path"
      val separator = json getAsString "separator"
      val field = json asString "field"
      frame.TextFileInput(path, separator, field)
    }
  }

  /**
   * Text File Output.
   */
  object TextFileOutputAdapter extends FrameStepAdapter("TextFileOutput", OUTPUT, One, One,
    "filename" -> TEXT, "field 0" -> TEXT, "format 0" -> TEXT,
    "separator" -> TEXT, "showHeader" -> BOOLEAN) {
    def makeStep(json: JsonObject) = {
      val filename = json asString "filename"
      val formats = json.asTupledList2[String, String]("@array") map {
        case (name, format) => name -> format
      }
      val separator = json getAsString "separator"
      val showHeader = json asBoolean "showHeader"
      frame.TextFileOutput(filename, formats)
    }
  }

  /**
   * Text Folder Input.
   */
  object TextFolderInputAdapter extends FrameStepAdapter("TextFolderInput", INPUT, Nil, One,
    "path" -> TEXT, "nameField" -> TEXT, "dataField" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val path = json asString "path"
      val nameField = json asString "nameField"
      val dataField = json asString "dataField"
      frame.TextFolderInput(path, nameField, dataField)
    }
  }

  /**
   * Union.
   */
  object Union extends FrameStepAdapter("Union", FLOW, Two, One) {
    def makeStep(json: JsonObject) = frame.Union()
  }

  /**
   * Column Stats.
   */
  object ColumnStatsAdapter extends FrameStepAdapter("ColumnStats", STATS, One, One,
    "field 0" -> TEXT, "groupBy" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val dataFields = json asStringList "@array"
      val groupFields = json getAsString "groupBy" map splitAndTrim(",") getOrElse Nil
      frame.mllib.ColumnStats(dataFields, groupFields)
    }
  }

  /**
   * Correlation.
   */
  object CorrelationAdapter extends FrameStepAdapter("Correlation", STATS, One, One,
    "field 0" -> TEXT, "groupBy" -> TEXT, "method" -> enum(frame.mllib.CorrelationMethod)) {
    def makeStep(json: JsonObject) = {
      val dataFields = json asStringList "@array"
      val groupFields = json getAsString "groupBy" map splitAndTrim(",") getOrElse Nil
      val method = json.asEnum[CorrelationMethod](frame.mllib.CorrelationMethod)("method")
      frame.mllib.Correlation(dataFields, groupFields, method)
    }
  }

  /**
   * Regression.
   */
  object RegressionAdapter extends FrameStepAdapter("Regression", STATS, One, One,
    "labelField" -> TEXT, "field 0" -> TEXT, "groupBy" -> TEXT,
    "method" -> enum(frame.mllib.RegressionMethod),
    "iterations" -> NUMBER, "step" -> NUMBER, "intercept" -> BOOLEAN) {
    def makeStep(json: JsonObject) = {
      val labelField = json asString "labelField"
      val dataFields = json asStringList "@array"
      val groupFields = json getAsString "groupBy" map splitAndTrim(",") getOrElse Nil
      val method = json.asEnum[RegressionMethod[_ <: GeneralizedLinearModel]](frame.mllib.RegressionMethod)("method")
      val iterationCount = json asInt "iterations"
      val stepSize = json asDouble "step"
      val allowIntercept = json asBoolean "intercept"
      val config = frame.mllib.RegressionConfig(method, iterationCount, stepSize, allowIntercept)
      frame.mllib.Regression(labelField, dataFields, groupFields, config)
    }
  }

  /**
   * DSA Input.
   */
  object DSAInputAdapter extends FrameStepAdapter("DSAInput", DSA, Nil, One,
    "path 0" -> TEXT, "type 0" -> DATA_TYPE) {
    def makeStep(json: JsonObject) = {
      val paths = json.asTupledList2[String, String]("@array") map {
        case (path, typeName) => path -> TypeUtils.typeForName(typeName)
      }
      step.DSAInput(paths)
    }
  }

  /**
   * DSA Output.
   */
  object DSAOutputAdapter extends FrameStepAdapter("DSAOutput", DSA, One, Nil,
    "field 0" -> TEXT, "path 0" -> TEXT) {
    def makeStep(json: JsonObject) = {
      val fields = json.asTupledList2[String, String]("@array") map {
        case (name, path) => name -> path
      }
      step.DSAOutput(fields)
    }
  }

  /**
   * List of available adapters retrieved through reflection.
   */
  val adapters = buildAdapterList[FrameBlockFactory.type]
}