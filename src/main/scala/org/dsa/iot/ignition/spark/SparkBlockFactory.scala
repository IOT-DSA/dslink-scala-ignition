package org.dsa.iot.ignition.spark

import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.ignition._
import org.dsa.iot.ignition.ParamInfo.input

import com.ignition.{ SparkHelper, frame }
import com.ignition.frame.BasicAggregator.{ BasicAggregator, valueToAggregator }

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
  }
  import Categories._

  implicit lazy val sparkRuntime = new frame.DefaultSparkRuntime(SparkHelper.sqlContext, true)

  /* input */

  object CsvFileInputAdapter extends AbstractRxBlockAdapter[CsvFileInput]("CsvInput", INPUT,
    "path" -> TEXT, "separator" -> TEXT default ",", "name 0" -> TEXT, "type 0" -> DATA_TYPE) {
    def createBlock(json: JsonObject) = CsvFileInput()
    def setupBlock(block: CsvFileInput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.path, json, "path", blocks)
      init(block.separator, json, "separator", blocks)
      set(block.columns, json, "@array")(extractStructFields(false))
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

  /* output */

  object CassandraOutputAdapter extends AbstractRxBlockAdapter[CassandraOutput]("CassandraOutput", OUTPUT,
    "keyspace" -> TEXT, "table" -> TEXT, input) {
    def createBlock(json: JsonObject) = CassandraOutput()
    def setupBlock(block: CassandraOutput, json: JsonObject, blocks: DSABlockMap) = {
      init(block.keyspace, json, "keyspace", blocks)
      init(block.table, json, "table", blocks)
      connect(block.source, json, "input", blocks)
    }
  }

  /* stats */

  object BasicStatsAdapter extends AbstractRxBlockAdapter[BasicStats]("BasicStats", STATS,
    "name 0" -> TEXT, "func 0" -> enum(frame.BasicAggregator), "groupBy" -> TEXT, input) {
    def createBlock(json: JsonObject) = BasicStats()
    def setupBlock(block: BasicStats, json: JsonObject, blocks: DSABlockMap) = {
      set(block.columns, json, "@array")(extractAggregatedFields)
      set(block.groupBy, json, "groupBy")(extractSeparatedStrings)
      connect(block.source, json, "input", blocks)
    }
    private def extractAggregatedFields(json: JsonObject, key: String) = json.asTupledList2[String, String](key) map {
      case (name, strFunc) => name -> (frame.BasicAggregator.withName(strFunc): BasicAggregator)
    }
  }

  /* transform */

  object SQLQueryAdapter extends AbstractRxBlockAdapter[SQLQuery]("SQLQuery", TRANSFORM,
    "query" -> TEXTAREA, "input 0" -> TABLE) {
    def createBlock(json: JsonObject) = new SQLQuery
    def setupBlock(block: SQLQuery, json: JsonObject, blocks: DSABlockMap) = {
      init(block.query, json, "query", blocks)
      connect(block.sources, json, "@array", blocks)
    }
  }
}