package org.dsa.iot.ignition

import com.typesafe.config.ConfigFactory

object Settings {
  private val rootConfig = ConfigFactory.load.getConfig("dslink-ignition")

  val df = rootConfig.getConfig("dataflow")

  val dfPath = df.getString("path")
  val dfCreateCmd = df.getString("create")
  val dfExportCmd = df.getString("export")
  val dfDeleteCmd = df.getString("delete")

  val dfCreatePath = dfPath + "/" + dfCreateCmd

  val dfDesignerKey = df.getString("designer-key")
}