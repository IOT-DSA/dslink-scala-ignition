package org.dsa.iot.ignition.spark

import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.ignition.{ AbstractRxBlockAdapter, DSARxBlock, TEXT, TypeConverters, tuple2Param }

import com.ignition.SparkHelper
import com.ignition.frame.DefaultSparkRuntime

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

  implicit val sparkRuntime = new DefaultSparkRuntime(SparkHelper.sqlContext)

  /* input */

  object CsvFileInput extends AbstractRxBlockAdapter[CsvFileInput]("CsvInput", INPUT,
    "path" -> TEXT, "separator" -> TEXT default ",", "name 0" -> TEXT, "type 0" -> DATA_TYPE) {
    def createBlock(json: JsonObject) = new CsvFileInput
    def setupBlock(block: CsvFileInput, json: JsonObject, blocks: Map[String, DSARxBlock]) = {
      init(block.path, json, "path", blocks)
      set(block.separator, json, "separator")
      set(block.columns, json, "@array")(extStructFields(false))
    }
  }

  /* output */

  /* stats */

  /* transform */
}