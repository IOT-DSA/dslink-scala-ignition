package org.dsa.iot.ignition.spark

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.spark.sql.{ DataFrame, Row, SaveMode }
import org.apache.spark.sql.types.{ StructField, StructType }
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.ignition._
import org.dsa.iot.ignition.ParamInfo.input
import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect
import org.dsa.iot.scala.valueToAny
import org.dsa.iot.util.Logging

import com.ignition.{ SparkHelper, frame }
import com.ignition.frame._
import com.ignition.frame.BasicAggregator.valueToAggregator
import com.ignition.frame.ReduceOp.valueToOp
import com.ignition.frame.mllib.{ CorrelationMethod, RegressionMethod }
import com.ignition.script.{ JsonPathExpression, MvelExpression, XPathExpression }
import com.ignition.types.TypeUtils

import org.apache.spark.mllib.regression.GeneralizedLinearModel

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
object SparkBlockFactory extends TypeConverters with Logging {

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

  implicit def anyToDataFrame(any: Any): DataFrame = any match {
    case x: DataFrame                => x
    case x: Iterable[_] if x.isEmpty => sparkRuntime.ctx.emptyDataFrame
    case x: Iterable[_] =>

      def unwrapped(x: Any): Any = x match {
        case v: Value       => valueToAny(v)
        case v: Iterable[_] => v map unwrapped
        case v              => v
      }

      val rows = x map unwrapped collect {
        case v: Iterable[_] => Row.fromSeq(v.toSeq)
        case v if v != null => Row(v)
      }

      rows.headOption map { row =>
        val fields = row.toSeq.zipWithIndex map {
          case (value, index) => StructField("c" + index, TypeUtils.typeForValue(value))
        }
        val schema = StructType(fields.toSeq)
        sparkRuntime.ctx.createDataFrame(rows.toList.asJava, schema)
      } getOrElse sparkRuntime.ctx.emptyDataFrame
    case x =>
      error(s"Unknown data type for $x: " + (if (x == null) "null" else x.getClass.getName))
      sparkRuntime.ctx.emptyDataFrame
  }
  implicit def anyToOptDataFrame(x: Any): Option[DataFrame] = Option(x) map anyToDataFrame

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

  object TextFileInputAdapter extends AbstractRxBlockAdapter[TextFileInput]("TextFileInput", INPUT,
    "path" -> TEXT, "separator" -> TEXT, "field" -> TEXT default "content") {
    def createBlock(json: JsonObject) = TextFileInput()
    def setupBlock(block: TextFileInput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.path, json, "path", blocks)
      init(block.separator, json, "separator", blocks)
      init(block.field, json, "field", blocks)
    }
  }

  object TextFolderInputAdapter extends AbstractRxBlockAdapter[TextFolderInput]("TextFolderInput",
    INPUT, "path" -> TEXT, "nameField" -> TEXT default "filename", "dataField" -> TEXT default "content") {
    def createBlock(json: JsonObject) = TextFolderInput()
    def setupBlock(block: TextFolderInput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.path, json, "path", blocks)
      init(block.nameField, json, "nameField", blocks)
      init(block.dataField, json, "dataField", blocks)
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

  object TextFileOutputAdapter extends TransformerAdapter[DataFrame, TextFileOutput]("TextFileOutput",
    OUTPUT, "filename" -> TEXT, "field" -> listOf(TEXT), "format" -> listOf(TEXT),
    "separator" -> TEXT default ",", "showHeader" -> BOOLEAN default true) {
    def createBlock(json: JsonObject) = TextFileOutput()
    def setupAttributes(block: TextFileOutput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.filename, json, "filename", blocks)
      set(block.formats, json, arrayField)(extractFormats)
      init(block.separator, json, "separator", blocks)
      init(block.header, json, "showHeader", blocks)
    }
    private def extractFormats(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
      case (name, value) => name -> value
    }
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

  object SetVariablesAdapter extends TransformerAdapter[DataFrame, SetVariables]("SetVariables", TRANSFORM,
    "name" -> listOf(TEXT), "type" -> listOf(DATA_TYPE), "value" -> listOf(TEXT)) {
    def createBlock(json: JsonObject) = SetVariables()
    def setupAttributes(block: SetVariables, json: JsonObject, blocks: DSABlockMap) = {
      set(block.vars, json, arrayField)(extractVars)
    }
    private def extractVars(json: JsonObject, key: String) = json.asTupledList3[String, String, String](key) map {
      case (name, typeName, strValue) => name -> parseValue(strValue, typeName)
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
    "name" -> listOf(TEXT), "func" -> listOf(enum(BasicAggregator)), "groupBy" -> TEXT) {
    def createBlock(json: JsonObject) = BasicStats()
    def setupAttributes(block: BasicStats, json: JsonObject, blocks: DSABlockMap) = {
      set(block.columns, json, arrayField)(extractAggregatedFields)
      set(block.groupBy, json, "groupBy")(extractSeparatedStrings)
    }
    private def extractAggregatedFields(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
      case (name, strFunc) => name -> (BasicAggregator.withName(strFunc): BasicAggregator.BasicAggregator)
    }
  }

  object ReduceAdapter extends TransformerAdapter[DataFrame, Reduce]("Reduce", AGGREGATE,
    "name" -> listOf(TEXT), "operation" -> listOf(enum(ReduceOp)), "groupBy" -> TEXT) {
    def createBlock(json: JsonObject) = Reduce()
    def setupAttributes(block: Reduce, json: JsonObject, blocks: DSABlockMap) = {
      set(block.columns, json, arrayField)(extractReducedFields)
      set(block.groupBy, json, "groupBy")(extractSeparatedStrings)
    }
    private def extractReducedFields(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
      case (name, strFunc) => name -> (ReduceOp.withName(strFunc): ReduceOp.ReduceOp)
    }
  }

  object ColumnStatsAdapter extends TransformerAdapter[DataFrame, ColumnStats]("ColumnStats", AGGREGATE,
    "field" -> listOf(TEXT), "groupBy" -> TEXT) {
    def createBlock(json: JsonObject) = ColumnStats()
    def setupAttributes(block: ColumnStats, json: JsonObject, blocks: DSABlockMap) = {
      set(block.columns, json, arrayField)(extractColumns)
      set(block.groupBy, json, "groupBy")(extractSeparatedStrings)
    }
    private def extractColumns(json: JsonObject, key: String) = json asStringList key
  }

  object CorrelationAdapter extends TransformerAdapter[DataFrame, Correlation]("Correlation", AGGREGATE,
    "field" -> listOf(TEXT), "groupBy" -> TEXT,
    "method" -> enum(CorrelationMethod) default CorrelationMethod.PEARSON) {
    def createBlock(json: JsonObject) = Correlation()
    def setupAttributes(block: Correlation, json: JsonObject, blocks: DSABlockMap) = {
      set(block.dataFields, json, arrayField)(extractFields)
      set(block.method, json, "method")(extractMethod)
      set(block.groupBy, json, "groupBy")(extractSeparatedStrings)
    }
    private def extractFields(json: JsonObject, key: String) = json asStringList key
    private def extractMethod(json: JsonObject, key: String) =
      json.asEnum[CorrelationMethod.CorrelationMethod](CorrelationMethod)(key)
  }

  object RegressionAdapter extends TransformerAdapter[DataFrame, Regression]("Regression", AGGREGATE,
    "labelField" -> TEXT, "field" -> listOf(TEXT), "groupBy" -> TEXT,
    "method" -> enum(RegressionMethod) default RegressionMethod.LINEAR,
    "iterations" -> NUMBER default 100, "step" -> NUMBER default 1, "intercept" -> BOOLEAN default false) {
    def createBlock(json: JsonObject) = Regression()
    def setupAttributes(block: Regression, json: JsonObject, blocks: DSABlockMap) = {
      init(block.labelField, json, "labelField", blocks)
      set(block.dataFields, json, arrayField)(extractFields)
      set(block.groupBy, json, "groupBy")(extractSeparatedStrings)
      set(block.method, json, "method")(extractMethod)
      init(block.iterationCount, json, "iterations", blocks)
      init(block.stepSize, json, "step", blocks)
      init(block.allowIntercept, json, "intercept", blocks)
    }
    private def extractFields(json: JsonObject, key: String) = json asStringList key
    private def extractMethod(json: JsonObject, key: String) =
      json.asEnum[RegressionMethod.RegressionMethod[_ <: GeneralizedLinearModel]](RegressionMethod)(key)
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