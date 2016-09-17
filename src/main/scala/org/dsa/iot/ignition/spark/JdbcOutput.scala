package org.dsa.iot.ignition.spark

import org.apache.spark.sql.SaveMode

import com.ignition.frame.SparkRuntime

/**
 * Writes data into a database table over JDBC.
 */
class JdbcOutput(implicit rt: SparkRuntime) extends RxFrameTransformer {
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
   * Creates a new JdbcOutput instance with the specified username, password, save mode and properties.
   */
  def apply(username: Option[String] = None, password: Option[String] = None,
            mode: SaveMode = SaveMode.Append,
            properties: Map[String, String] = Map.empty)(implicit rt: SparkRuntime): JdbcOutput = {
    val block = new JdbcOutput
    block.username <~ username
    block.password <~ password
    block.mode <~ mode
    block.properties <~ (properties.toSeq: _*)
    block
  }
}