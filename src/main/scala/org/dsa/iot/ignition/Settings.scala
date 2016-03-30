package org.dsa.iot.ignition

import com.typesafe.config.ConfigFactory
import com.ignition.util.ConfigUtils.RichConfig
import scala.concurrent.duration.{ Duration, MILLISECONDS }

object Settings {
  private val rootConfig = ConfigFactory.load.getConfig("dslink-ignition")

  val df = rootConfig.getConfig("dataflow")

  val dfPath = df.getString("path")
  val dfCreateCmd = df.getString("create")
  val dfExportCmd = df.getString("export")
  val dfDeleteCmd = df.getString("delete")

  val dfCreatePath = dfPath + "/" + dfCreateCmd

  val dfDesignerKey = df.getString("designer-key")
  
  val maxTimeout = {
    val millis = rootConfig.getTimeInterval("max-timeout").getMillis
    Duration.apply(millis, MILLISECONDS)
  }
}