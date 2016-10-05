package org.dsa.iot.ignition.spark

import org.apache.spark.sql.DataFrame
import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.Having

import com.ignition.frame.SparkRuntime

import rx.lang.scala.Observable

/**
 * Reads data from a JDBC source.
 */
class JdbcInput(implicit rt: SparkRuntime) extends AbstractRxBlock[DataFrame] {

  def url(str: String): JdbcInput = this having (url <~ str)
  def username(user: String): JdbcInput = this having (username <~ Some(user))
  def password(pwd: String): JdbcInput = this having (password <~ Some(pwd))
  def sql(str: String): JdbcInput = this having (sql <~ str)
  def properties(props: (String, String)*): JdbcInput = this having (properties <~ props)

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
   * Creates a new JdbcInput instance without username, password or properties.
   */
  def apply()(implicit rt: SparkRuntime): JdbcInput = {
    val block = new JdbcInput
    block.username <~ None
    block.password <~ None
    block.properties <~ Map.empty
    block
  }
}