package org.dsa.iot.ignition

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.dsa.iot.dslink.util.json.JsonObject

import com.ignition.util.Logging

import _root_.rx.lang.scala.Subscriber

/**
 * A set of interconnected RX blocks forming a dataflow.
 */
class RxFlow(val name: String) extends Logging {

  private var running = false
  private var blocks = Map.empty[String, DSARxBlock]

  def isRunning = running

  def allBlocks = blocks.toMap

  def restart() = synchronized {
    debug(s"Resetting flow [$name] blocks...")
    blocks.values foreach (_.reset)
    running = true
    info(s"Flow [$name] restarted")
  }

  def shutdown() = synchronized {
    if (!isRunning)
      warn(s"Flow [$name] is not running, shutdown ignored")
    else {
      debug(s"Shutting down flow [$name]...")
      blocks.values foreach (_.shutdown)
      running = false
      info(s"Flow [$name] shut down")
    }
  }

  def update(json: JsonObject) = synchronized {
    if (isRunning)
      shutdown

    debug(s"Starting flow [$name] update...")

    val jsonBlocks = json.getMap.asScala.toMap collect {
      case (name, obj: JsonObject) => name -> obj
    }

    val newBlocks: Map[String, DSARxBlock] = jsonBlocks map {
      case (name, json) =>
        val typeName = json.get[String]("customType")
        val adapter = RxBlockFactory.adapterMap(typeName)
        val block = adapter.createBlock(json)
        name -> block
    } toMap

    jsonBlocks foreach {
      case (name, json) =>
        val typeName = json.get[String]("customType")
        val adapter = RxBlockFactory.adapterMap(typeName).asInstanceOf[RxBlockAdapter[DSARxBlock]]
        val block = newBlocks(name)
        adapter.setupBlock(block, json, newBlocks)
    }

    newBlocks foreach {
      case (name, block) => block.output subscribe logSub(name)
    }
    
    this.blocks = newBlocks

    info(s"Flow [$name] update complete")
  }

  private def logSub(name: String) = Subscriber[Any](
    (x: Any) => debug(name + ": " + x),
    (err: Throwable) => error(s"$name: $err", err),
    () => info(s"$name: done"))
}