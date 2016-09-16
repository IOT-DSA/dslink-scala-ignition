package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime

/**
 * Writes documents into a MongoDB collection.
 */
class MongoOutput(implicit rt: SparkRuntime) extends RxFrameTransformer {
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