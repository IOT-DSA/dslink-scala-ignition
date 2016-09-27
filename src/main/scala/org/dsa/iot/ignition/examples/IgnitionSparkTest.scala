package org.dsa.iot.ignition.examples

import org.dsa.iot.ignition.spark.{ CsvFileInput, Join, SQLQuery }
import org.dsa.iot.rx.RichTuple2

import com.ignition.SparkHelper
import com.ignition.frame.{ DefaultSparkRuntime, JoinType }
import com.ignition.types.{ boolean, int, string }

object IgnitionSparkTest extends App {
  implicit val rt = new DefaultSparkRuntime(SparkHelper.sqlContext)

  val people = CsvFileInput()
  people.path <~ "/tmp/people.csv"
  people.separator <~ Some(",")
  people.columns <~ (string("name"), string("gender"), int("age"), boolean("married"))

  val scores = CsvFileInput()
  scores.path <~ "/tmp/scores.csv"
  scores.separator <~ Some(",")
  scores.columns <~ (string("student"), string("task"), int("score"))

  val join = Join()
  join.condition <~ "student = name"
  join.joinType <~ JoinType.LEFT

  val sql = SQLQuery()
  sql.query <~ """SELECT name, age, ROUND(AVG(score)) AS score, COUNT(score) as count 
    FROM input0 GROUP by name, age"""

  (people, scores) ~> join ~> sql.sources(0)

  sql.output subscribe (_.show)

  people.reset
  scores.reset
}