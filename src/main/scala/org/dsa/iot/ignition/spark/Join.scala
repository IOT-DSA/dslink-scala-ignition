package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.RxMerger2
import org.dsa.iot.scala.Having

import com.ignition.frame.{ SparkRuntime, JoinType }

/**
 * Performs join of the two data frames.
 * In row conditions, if there is ambiguity in a field's name, use "input0" and "input1"
 * prefixes for the first and second input respectively.
 */
class Join(implicit rt: SparkRuntime) extends RxMerger2[DataFrame, DataFrame, DataFrame] {

  def condition(str: String): Join = this having (condition <~ str)
  def joinType(jt: JoinType.JoinType): Join = this having (joinType <~ jt)

  val condition = Port[String]("condition")
  val joinType = Port[JoinType.JoinType]("joinType")

  protected def compute = (condition.in combineLatest joinType.in) flatMap {
    case (cond, jt) =>
      val join = com.ignition.frame.Join(cond, jt)
      (source1.in combineLatest source2.in) map {
        case (x, y) =>
          producer(x) --> join.in(0)
          producer(y) --> join.in(1)
          join.output
      }
  }
}

/**
 * Factory for [[Join]] instances.
 */
object Join {

  /**
   * Createa a new Join instance with INNER join type.
   */
  def apply()(implicit rt: SparkRuntime): Join = new Join joinType JoinType.INNER
}