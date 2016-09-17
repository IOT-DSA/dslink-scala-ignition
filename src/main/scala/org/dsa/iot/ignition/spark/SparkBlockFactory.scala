package org.dsa.iot.ignition.spark

import org.apache.spark.sql.{ DataFrame, SaveMode }
import org.apache.spark.sql.types.StructField
import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.ignition._
import org.dsa.iot.ignition.ParamInfo.input
import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect
import com.ignition.{ SparkHelper, frame }
import com.ignition.frame.{ FrameSplitter, FrameTransformer, JoinType }
import com.ignition.frame.BasicAggregator.{ BasicAggregator, valueToAggregator }
import com.ignition.frame.ReduceOp.{ ReduceOp, valueToOp }
import com.ignition.frame.SparkRuntime
import com.ignition.script.{ JsonPathExpression, MvelExpression, XPathExpression }
import com.ignition.types.TypeUtils
import com.ignition.frame.HttpMethod

/**
 * RX Transformer built on top of an Ignition FrameTransformer class, works as a bridge
 * between ignition transformers and RX transformers.
 */
abstract class RxFrameTransformer extends RxTransformer[DataFrame, DataFrame] {

  protected def doTransform(tx: FrameTransformer)(implicit rt: SparkRuntime) = source.in map { df =>
    val source = producer(df)
    source --> tx
    tx.output
  }

  protected def doTransform(tx: FrameSplitter)(implicit rt: SparkRuntime) = source.in map { df =>
    val source = producer(df)
    source --> tx
    tx.output
  }
}

/**
 * Spark RX blocks.
 */
object SparkBlockFactory extends TypeConverters {

  object Categories {
    val INPUT = "Spark.Input"
    val OUTPUT = "Spark.Output"
    val TRANSFORM = "Spark.Transform"
    val FILTER = "Spark.Filter"
    val COMBINE = "Spark.Combine"
    val AGGREGATE = "Spark.Aggregate"
    val UTIL = "Spark.Utilities"
  }
  import Categories._

  implicit lazy val sparkRuntime = new frame.DefaultSparkRuntime(SparkHelper.sqlContext, true)

  /* input */

  object CsvFileInputAdapter extends AbstractRxBlockAdapter[CsvFileInput]("CsvInput", INPUT,
    "path" -> TEXT, "separator" -> TEXT default ",", "name" -> listOf(TEXT), "type" -> listOf(DATA_TYPE)) {
    def createBlock(json: JsonObject) = CsvFileInput()
    def setupBlock(block: CsvFileInput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.path, json, "path", blocks)
      init(block.separator, json, "separator", blocks)
      set(block.columns, json, arrayField)(extractStructFields(false))
    }
  }

  object CassandraInputAdapter extends AbstractRxBlockAdapter[CassandraInput](
    "CassandraInput", INPUT, "keyspace" -> TEXT, "table" -> TEXT, "columns" -> TEXT, "where" -> TEXTAREA) {
    def createBlock(json: JsonObject) = CassandraInput()
    def setupBlock(block: CassandraInput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.keyspace, json, "keyspace", blocks)
      init(block.table, json, "table", blocks)
      set(block.columns, json, "columns")(extractSeparatedStrings)
      init(block.where, json, "where", blocks)
    }
  }

  object MongoInputAdapter extends AbstractRxBlockAdapter[MongoInput]("MongoInput", INPUT,
    "database" -> TEXT, "collection" -> TEXT,
    "name" -> listOf(TEXT), "type" -> listOf(DATA_TYPE), "nullable" -> listOf(BOOLEAN),
    "sort" -> TEXT, "limit" -> NUMBER, "offset" -> NUMBER) {
    def createBlock(json: JsonObject) = MongoInput()
    def setupBlock(block: MongoInput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.database, json, "database", blocks)
      init(block.collection, json, "collection", blocks)
      set(block.columns, json, arrayField)(extractStructFields(true))
      set(block.sort, json, "sort")(extractSort)
      init(block.limit, json, "limit", blocks)
      init(block.offset, json, "offset", blocks)
    }
    private def extractSort(json: JsonObject, key: String) = json getAsString key map splitAndTrim(",") getOrElse Nil map (str =>
      str split "\\s+" match {
        case Array(name)      => frame.SortOrder(name)
        case Array(name, dir) => frame.SortOrder(name, dir.toLowerCase startsWith "asc")
      })
  }

  object JdbcInputAdapter extends AbstractRxBlockAdapter[JdbcInput]("JdbcInput", INPUT,
    "url" -> TEXT, "username" -> TEXT, "password" -> TEXT, "sql" -> TEXTAREA,
    "propName" -> listOf(TEXT), "propValue" -> listOf(TEXT)) {
    def createBlock(json: JsonObject) = JdbcInput()
    def setupBlock(block: JdbcInput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.url, json, "url", blocks)
      init(block.username, json, "username", blocks)
      init(block.password, json, "password", blocks)
      init(block.sql, json, "sql", blocks)
      set(block.properties, json, arrayField)(extractProperties)
    }
    private def extractProperties(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
      case (name, value) => name -> value
    }
  }

  object JsonFileInputAdapter extends AbstractRxBlockAdapter[JsonFileInput]("JsonFileInput", INPUT,
    "path" -> TEXT, "name" -> listOf(TEXT), "jsonPath" -> listOf(TEXT)) {
    def createBlock(json: JsonObject) = JsonFileInput()
    def setupBlock(block: JsonFileInput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.path, json, "path", blocks)
      set(block.fields, json, arrayField)(extractFields)
    }
    private def extractFields(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
      case (name, value) => name -> value
    }
  }

  object DataGridAdapter extends AbstractRxBlockAdapter[DataGrid]("DataGrid", INPUT,
    "schema" -> TEXT, "row" -> listOf(TEXT)) {
    def createBlock(json: JsonObject) = DataGrid()
    def setupBlock(block: DataGrid, json: JsonObject, blocks: DSABlockMap) = {
      set(block.columns, json, "schema")(extractSchemaFields)
      set(block.rows, json, "row")(extractRowData(extractSchemaFields(json, "schema")))
    }
    private def extractSchemaFields(json: JsonObject, key: String) = splitAndTrim(",")(json asString key) map { s =>
      val Array(name, typeName) = s.split(":")
      new StructField(name, TypeUtils.typeForName(typeName), true)
    }
    private def extractRowData(fields: Seq[StructField])(json: JsonObject, key: String) = json asStringList key map { s =>
      val parts = s.split(",").map(_.trim)
      val items = fields zip parts map {
        case (field, value) => TypeUtils.valueOf(value, field.dataType)
      }
      org.apache.spark.sql.Row.fromSeq(items)
    }
  }

  /* output */

  object CassandraOutputAdapter extends TransformerAdapter[DataFrame, CassandraOutput]("CassandraOutput", OUTPUT,
    "keyspace" -> TEXT, "table" -> TEXT) {
    def createBlock(json: JsonObject) = CassandraOutput()
    def setupAttributes(block: CassandraOutput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.keyspace, json, "keyspace", blocks)
      init(block.table, json, "table", blocks)
    }
  }

  object MongoOutputAdapter extends TransformerAdapter[DataFrame, MongoOutput]("MongoOutput", OUTPUT,
    "database" -> TEXT, "collection" -> TEXT) {
    def createBlock(json: JsonObject) = MongoOutput()
    def setupAttributes(block: MongoOutput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.database, json, "database", blocks)
      init(block.collection, json, "collection", blocks)
    }
  }

  object JdbcOutputAdapter extends TransformerAdapter[DataFrame, JdbcOutput]("JdbcOutput", OUTPUT,
    "url" -> TEXT, "username" -> TEXT, "password" -> TEXT, "table" -> TEXT,
    "mode" -> enum(SaveMode.values) default SaveMode.Append,
    "propName" -> listOf(TEXT), "propValue" -> listOf(TEXT)) {
    def createBlock(json: JsonObject) = JdbcOutput()
    def setupAttributes(block: JdbcOutput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.url, json, "url", blocks)
      init(block.username, json, "username", blocks)
      init(block.password, json, "password", blocks)
      init(block.table, json, "table", blocks)
      set(block.mode, json, "mode")(extractMode)
      set(block.properties, json, arrayField)(extractProperties)
    }
    private def extractProperties(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
      case (name, value) => name -> value
    }
    private def extractMode(json: JsonObject, key: String) = SaveMode.valueOf(json asString key)
  }

  /* transform */

  object AddFieldsAdapter extends TransformerAdapter[DataFrame, AddFields]("AddFields", TRANSFORM,
    "name" -> listOf(TEXT), "type" -> listOf(DATA_TYPE), "value" -> listOf(TEXT)) {
    def createBlock(json: JsonObject) = AddFields()
    def setupAttributes(block: AddFields, json: JsonObject, blocks: DSABlockMap) = {
      set(block.fields, json, arrayField)(extractFields)
    }
    private def extractFields(json: JsonObject, key: String) = json.asTupledList3[String, String, String](key) map {
      case (name, typeName, strValue) => name -> parseValue(strValue, typeName)
    }
  }

  object FormulaAdapter extends TransformerAdapter[DataFrame, Formula]("Formula", TRANSFORM,
    "name" -> listOf(TEXT), "dialect" -> listOf(enum(ScriptDialect)),
    "expression" -> listOf(TEXT), "source" -> listOf(TEXT)) {
    def createBlock(json: JsonObject) = Formula()
    def setupAttributes(block: Formula, json: JsonObject, blocks: DSABlockMap) = {
      set(block.fields, json, arrayField)(extractFields)
    }
    private def extractFields(json: JsonObject, key: String) = json.asTupledList4[String, String, String, String](key) map {
      case (name, ScriptDialect.MVEL.name, expression, src)  => name -> MvelExpression(expression)
      case (name, ScriptDialect.XPATH.name, expression, src) => name -> XPathExpression(expression, src)
      case (name, ScriptDialect.JPATH.name, expression, src) => name -> JsonPathExpression(expression, src)
    }
  }

  object RestClientAdapter extends TransformerAdapter[DataFrame, RestClient]("RestClient", TRANSFORM,
    "url" -> TEXT, "method" -> enum(HttpMethod) default HttpMethod.GET,
    "body" -> TEXTAREA, "header" -> listOf(TEXT), "headerValue" -> listOf(TEXT),
    "resultField" -> TEXT default "result", "statusField" -> TEXT default "status", "headersField" -> TEXT) {
    def createBlock(json: JsonObject) = RestClient()
    def setupAttributes(block: RestClient, json: JsonObject, blocks: DSABlockMap) = {
      init(block.url, json, "url", blocks)
      set(block.method, json, "method")(extractMethod)
      init(block.body, json, "body", blocks)
      set(block.headers, json, arrayField)(extractHeaders)
      init(block.resultField, json, "resultField", blocks)
      init(block.statusField, json, "statusField", blocks)
      init(block.headersField, json, "headersField", blocks)
    }
    private def extractMethod(json: JsonObject, key: String) = json.asEnum[HttpMethod.HttpMethod](HttpMethod)(key)
    private def extractHeaders(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
      case (name, value) => name -> value
    }
  }

  object SelectValuesAdapter extends TransformerAdapter[DataFrame, SelectValues]("SelectValues", TRANSFORM,
    "action" -> listOf(enum("retain", "rename", "remove", "retype")), "data" -> listOf(TEXT)) {
    def createBlock(json: JsonObject) = SelectValues()
    def setupAttributes(block: SelectValues, json: JsonObject, blocks: DSABlockMap) = {
      set(block.actions, json, arrayField)(extractActions)
    }
    private def extractActions(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
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
          field -> TypeUtils.typeForName(typeName)
        }
        frame.SelectAction.Retype(pairs.toMap)
    }
  }

  /* filter */

  object FilterAdapter extends TransformerAdapter[DataFrame, Filter]("Filter", FILTER, "condition" -> TEXTAREA) {
    def createBlock(json: JsonObject) = Filter()
    def setupAttributes(block: Filter, json: JsonObject, blocks: DSABlockMap) = {
      init(block.condition, json, "condition", blocks)
    }
  }

  /* combine */

  object SQLQueryAdapter extends AbstractRxBlockAdapter[SQLQuery]("SQLQuery", COMBINE,
    "query" -> TEXTAREA, "input" -> listOf(TABLE)) {
    def createBlock(json: JsonObject) = SQLQuery()
    def setupBlock(block: SQLQuery, json: JsonObject, blocks: DSABlockMap) = {
      init(block.query, json, "query", blocks)
      connect(block.sources, json, arrayField, blocks)
    }
  }

  object IntersectionAdapter extends AbstractRxBlockAdapter[Intersection]("Intersection", COMBINE,
    "input" -> listOf(TABLE)) {
    def createBlock(json: JsonObject) = Intersection()
    def setupBlock(block: Intersection, json: JsonObject, blocks: DSABlockMap) = {
      connect(block.sources, json, arrayField, blocks)
    }
  }

  object UnionAdapter extends AbstractRxBlockAdapter[Union]("Union", COMBINE,
    "input" -> listOf(TABLE)) {
    def createBlock(json: JsonObject) = Union()
    def setupBlock(block: Union, json: JsonObject, blocks: DSABlockMap) = {
      connect(block.sources, json, arrayField, blocks)
    }
  }

  object JoinAdapter extends AbstractRxBlockAdapter[Join]("Join", COMBINE,
    "condition" -> TEXTAREA, "joinType" -> enum(JoinType) default JoinType.INNER, input(1), input(2)) {
    def createBlock(json: JsonObject) = Join()
    def setupBlock(block: Join, json: JsonObject, blocks: DSABlockMap) = {
      init(block.condition, json, "condition", blocks)
      set(block.joinType, json, "joinType")(extJoinType)
      connect(block.source1, json, "input1", blocks)
      connect(block.source2, json, "input2", blocks)
    }
    private def extJoinType(json: JsonObject, key: String) = json.asEnum[JoinType.JoinType](JoinType)(key)
  }

  /* aggregate */

  object BasicStatsAdapter extends TransformerAdapter[DataFrame, BasicStats]("BasicStats", AGGREGATE,
    "name" -> listOf(TEXT), "func" -> listOf(enum(frame.BasicAggregator)), "groupBy" -> TEXT) {
    def createBlock(json: JsonObject) = BasicStats()
    def setupAttributes(block: BasicStats, json: JsonObject, blocks: DSABlockMap) = {
      set(block.columns, json, arrayField)(extractAggregatedFields)
      set(block.groupBy, json, "groupBy")(extractSeparatedStrings)
    }
    private def extractAggregatedFields(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
      case (name, strFunc) => name -> (frame.BasicAggregator.withName(strFunc): BasicAggregator)
    }
  }

  object ReduceAdapter extends TransformerAdapter[DataFrame, Reduce]("Reduce", AGGREGATE,
    "name" -> listOf(TEXT), "operation" -> listOf(enum(frame.ReduceOp)), "groupBy" -> TEXT) {
    def createBlock(json: JsonObject) = Reduce()
    def setupAttributes(block: Reduce, json: JsonObject, blocks: DSABlockMap) = {
      set(block.columns, json, arrayField)(extractReducedFields)
      set(block.groupBy, json, "groupBy")(extractSeparatedStrings)
    }
    private def extractReducedFields(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
      case (name, strFunc) => name -> (frame.ReduceOp.withName(strFunc): ReduceOp)
    }
  }

  /* utilities */

  object CacheAdapter extends TransformerAdapter[DataFrame, Cache]("Cache", UTIL) {
    def createBlock(json: JsonObject) = Cache()
    def setupAttributes(block: Cache, json: JsonObject, blocks: DSABlockMap) = {}
  }

  object DebugOutputAdapter extends TransformerAdapter[DataFrame, DebugOutput]("Debug", UTIL,
    "showNames" -> BOOLEAN, "showTypes" -> BOOLEAN, "title" -> TEXT, "maxWidth" -> NUMBER default 80) {
    def createBlock(json: JsonObject) = DebugOutput()
    def setupAttributes(block: DebugOutput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.showNames, json, "showNames", blocks)
      init(block.showTypes, json, "showTypes", blocks)
      init(block.title, json, "title", blocks)
      init(block.maxWidth, json, "maxWidth", blocks)
    }
  }

  object RepartitionAdapter extends TransformerAdapter[DataFrame, Repartition]("Repartition", UTIL,
    "size" -> NUMBER default 8, "shuffle" -> BOOLEAN default false) {
    def createBlock(json: JsonObject) = Repartition()
    def setupAttributes(block: Repartition, json: JsonObject, blocks: DSABlockMap) = {
      init(block.size, json, "size", blocks)
      init(block.shuffle, json, "shuffle", blocks)
    }
  }
}