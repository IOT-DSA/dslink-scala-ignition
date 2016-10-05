package org.dsa.iot.ignition.spark

import org.apache.spark.sql.SaveMode

import com.ignition.frame.SparkRuntime
import org.dsa.iot.scala.Having

/**
 * Writes data into a database table over JDBC.
 */
class JdbcOutput(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def url(str: String): JdbcOutput = this having (url <~ str)
  def username(user: String): JdbcOutput = this having (username <~ Some(user))
  def password(pwd: String): JdbcOutput = this having (password <~ Some(pwd))
  def table(tbl: String): JdbcOutput = this having (table <~ tbl)
  def mode(sm: SaveMode): JdbcOutput = this having (mode <~ sm)
  def properties(props: (String, String)*): JdbcOutput = this having (properties <~ props)

  val url = Port[String]("url")
  val username = Port[Option[String]]("username")
  val password = Port[Option[String]]("password")
  val table = Port[String]("table")
  val mode = Port[SaveMode]("mode")
  val properties = PortList[(String, String)]("properties")

  protected def compute = (url.in combineLatest username.in combineLatest password.in
    combineLatest table.in combineLatest mode.in combineLatest properties.combinedIns) flatMap {
      case (((((url, username), password), table), mode), props) =>
        doTransform(com.ignition.frame.JdbcOutput(url, table, mode, username, password, props.toMap))
    }
}

/**
 * Factory for [[JdbcOutput]] instances.
 */
object JdbcOutput {

  /**
   * Creates a new JdbcOutput instance with the no username, password or properties
   * and save mode set to `Append`.
   */
  def apply()(implicit rt: SparkRuntime): JdbcOutput = {
    val block = new JdbcOutput
    block.username <~ None
    block.password <~ None
    block.mode <~ SaveMode.Append
    block.properties <~ Map.empty
    block
  }
}