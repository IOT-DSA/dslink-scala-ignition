package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import org.dsa.iot.scala.Having

/**
 * Writes documents into a MongoDB collection.
 */
class MongoOutput(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def database(db: String): MongoOutput = this having (database <~ db)
  def collection(coll: String): MongoOutput = this having (collection <~ coll)

  val database = Port[String]("database")
  val collection = Port[String]("collection")

  protected def compute = (database.in combineLatest collection.in) flatMap {
    case (db, coll) => doTransform(com.ignition.frame.MongoOutput(db, coll))
  }
}

/**
 * Factory for [[MongoOutput]] instances.
 */
object MongoOutput {

  /**
   * Creates a new MongoOutput instance.
   */
  def apply()(implicit rt: SparkRuntime): MongoOutput = new MongoOutput
}