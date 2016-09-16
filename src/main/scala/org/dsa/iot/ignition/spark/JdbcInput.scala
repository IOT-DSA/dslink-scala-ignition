package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.AbstractRxBlock

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Reads data from a JDBC source.
 */
class JdbcInput(implicit rt: SparkRuntime) extends AbstractRxBlock[DataFrame] {
  val url = Port[String]("url")
  val username = Port[Option[String]]("username")
  val password = Port[Option[String]]("password")
  val sql = Port[String]("sql")
  val properties = PortList[(String, String)]("properties")

  protected def compute = (url.in combineLatest username.in combineLatest password.in
    combineLatest sql.in combineLatest properties.combinedIns) flatMap {
      case ((((url, username), password), sql), props) =>
        val ji = com.ignition.frame.JdbcInput(url, sql, username, password, props.toMap)
        Observable.just(ji.output)
    }
}

/**
 * Factory for [[JdbcInput]] instances.
 */
object JdbcInput {

  /**
   * Creates a new JdbcInput instance with the specified username, password and properties.
   */
  def apply(username: Option[String] = None, password: Option[String] = None,
            properties: Map[String, String] = Map.empty)(implicit rt: SparkRuntime): JdbcInput = {
    val block = new JdbcInput
    block.username <~ username
    block.password <~ password
    block.properties <~ (properties.toSeq: _*)
    block
  }
}