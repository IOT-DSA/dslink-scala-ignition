package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.RxTransformer

import com.ignition.frame.BasicAggregator.BasicAggregator
import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Calculates basic statistics.
 */
class BasicStats(implicit rt: SparkRuntime) extends RxTransformer[DataFrame, DataFrame] {
  val columns = PortList[(String, BasicAggregator)]("columns")
  val groupBy = Port[List[String]]("groupBy")

  protected def compute = {
    val fields = Observable.combineLatest(columns.ins.toIterable)(identity)
    (groupBy.in combineLatest fields) flatMap {
      case (grp, flds) =>
        val bs = com.ignition.frame.BasicStats(flds, grp)
        source.in map { df =>
          val source = producer(df)
          source --> bs
          bs.output
        }
    }
  }
}

/**
 * Factory for [[BasicStats]] instances.
 */
object BasicStats {

  /**
   * Creates a new BasicStats instance with the specified GROUP BY columns.
   */
  def apply(groupBy: List[String] = Nil)(implicit rt: SparkRuntime): BasicStats = {
    val block = new BasicStats
    block.groupBy <~ groupBy
    block
  }
}