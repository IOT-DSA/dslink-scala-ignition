package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ StructField, StructType }
import org.dsa.iot.rx.AbstractRxBlock

import com.ignition.frame.{ Page, SortOrder, SparkRuntime }

import rx.lang.scala.Observable

/**
 * Reads documents from MongoDB.
 */
class MongoInput(implicit rt: SparkRuntime) extends AbstractRxBlock[DataFrame] {
  val database = Port[String]("database")
  val collection = Port[String]("collection")
  val columns = PortList[StructField]("columns")
  val sort = Port[List[SortOrder]]("sort")
  val limit = Port[Int]("limit")
  val offset = Port[Int]("offset")

  protected def compute = database.in combineLatest collection.in combineLatest
    columns.combinedIns combineLatest sort.in combineLatest limit.in combineLatest offset.in flatMap {
      case (((((db, coll), fields), sort), limit), offset) =>
        val schema = StructType(fields)
        val mia = com.ignition.frame.MongoInput(db, coll, schema, Map.empty, sort, Page(limit, offset))
        Observable.just(mia.output)
    }
}

/**
 * Factory for [[MongoInput]] instances.
 */
object MongoInput {

  /**
   * Creates a new MongoInput instance with the specified sorting order, limit and offset.
   */
  def apply(sort: List[SortOrder] = Nil, limit: Int = Page.default.limit,
            offset: Int = Page.default.offset)(implicit rt: SparkRuntime): MongoInput = {
    val block = new MongoInput
    block.sort <~ sort
    block.limit <~ limit
    block.offset <~ offset
    block
  }
}