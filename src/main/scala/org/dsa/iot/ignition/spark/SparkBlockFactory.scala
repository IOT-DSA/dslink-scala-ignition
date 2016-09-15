package org.dsa.iot.ignition.spark

import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.ignition._
import org.dsa.iot.ignition.ParamInfo.input
import com.ignition.{ SparkHelper, frame }
import com.ignition.frame.BasicAggregator.{ BasicAggregator, valueToAggregator }
import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.RxTransformer
import com.ignition.frame.FrameTransformer
import com.ignition.frame.SparkRuntime
import org.apache.spark.sql.types.StructField
import com.ignition.types.TypeUtils

abstract class RxFrameTransformer extends RxTransformer[DataFrame, DataFrame] {

  protected def doTransform(tx: FrameTransformer)(implicit rt: SparkRuntime) = source.in map { df =>
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

  object SQLQueryAdapter extends AbstractRxBlockAdapter[SQLQuery]("SQLQuery", TRANSFORM,
    "query" -> TEXTAREA, "input" -> listOf(TABLE)) {
    def createBlock(json: JsonObject) = SQLQuery()
    def setupBlock(block: SQLQuery, json: JsonObject, blocks: DSABlockMap) = {
      init(block.query, json, "query", blocks)
      connect(block.sources, json, arrayField, blocks)
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
}