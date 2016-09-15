package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField
import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.ignition._
import org.dsa.iot.ignition.ParamInfo.input
import org.dsa.iot.rx.RxTransformer
import org.dsa.iot.rx.script.ScriptDialect
import com.ignition.{ SparkHelper, frame }
import com.ignition.frame.{ FrameSplitter, FrameTransformer, SparkRuntime }
import com.ignition.frame.BasicAggregator.{ BasicAggregator, valueToAggregator }
import com.ignition.script.{ JsonPathExpression, MvelExpression, XPathExpression }
import com.ignition.types.TypeUtils
import com.ignition.frame.JoinType

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
    val STATS = "Spark.Statistics"
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
    "keyspace" -> TEXT, "table" -> TEXT, input) {
    def createBlock(json: JsonObject) = CassandraOutput()
    def setupAttributes(block: CassandraOutput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.keyspace, json, "keyspace", blocks)
      init(block.table, json, "table", blocks)
    }
  }

  /* stats */

  object BasicStatsAdapter extends TransformerAdapter[DataFrame, BasicStats]("BasicStats", STATS,
    "name" -> listOf(TEXT), "func" -> listOf(enum(frame.BasicAggregator)), "groupBy" -> TEXT, input) {
    def createBlock(json: JsonObject) = BasicStats()
    def setupAttributes(block: BasicStats, json: JsonObject, blocks: DSABlockMap) = {
      set(block.columns, json, arrayField)(extractAggregatedFields)
      set(block.groupBy, json, "groupBy")(extractSeparatedStrings)
    }
    private def extractAggregatedFields(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
      case (name, strFunc) => name -> (frame.BasicAggregator.withName(strFunc): BasicAggregator)
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
}