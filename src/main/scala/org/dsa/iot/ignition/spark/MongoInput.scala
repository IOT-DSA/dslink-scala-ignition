package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ StructField, StructType }
import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.Having

import com.ignition.frame.{ Page, SortOrder, SparkRuntime }

import rx.lang.scala.Observable

/**
 * Reads documents from MongoDB.
 */
class MongoInput(implicit rt: SparkRuntime) extends AbstractRxBlock[DataFrame] {

  def database(db: String): MongoInput = this having (database <~ db)
  def collection(coll: String): MongoInput = this having (collection <~ coll)
  def orderBy(tuples: (String, Boolean)*): MongoInput = this having {
    sort <~ tuples.map(t => SortOrder(t._1, t._2)).toList
  }
  def limit(n: Int): MongoInput = this having (limit <~ n)
  def offset(n: Int): MongoInput = this having (offset <~ n)

  def columns(cols: StructField*): MongoInput = this having (columns <~ cols)
  def schema(schema: StructType): MongoInput = this having (columns <~ schema)

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
   * Creates a new MongoInput instance without limit, offset and order specification.
   */
  def apply()(implicit rt: SparkRuntime): MongoInput = {
    new MongoInput orderBy () limit Page.default.limit offset Page.default.offset
  }
}