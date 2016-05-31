package org.dsa.iot.rx

import concurrent.duration._
import scala.collection.JavaConverters._
import org.dsa.iot.dslink.node.actions.table._
import org.dsa.iot.dslink.node.value._
import org.dsa.iot.dslink.util.json._
import org.dsa.iot.rx.block._
import rx.lang.scala.Subscriber

/**
 * Contains interconnected RxBlocks.
 */
class RxFlow extends Logging {

  var blocksByName: Map[String, DSARxBlock] = Map.empty

  def reset() = {}
  
  def shutdown() = blocksByName.values foreach (_.shutdown)

  def update(json: JsonObject) = {
    debug("Starting flow update...")

    shutdown

    val jsonBlocks = json.getMap.asScala.toMap collect {
      case (name, obj: JsonObject) => name -> obj
    }

    blocksByName = jsonBlocks map {
      case (name, json) =>
        val typeName = json.get[String]("customType")
        val adapter = BlockFactory.adapterMap(typeName)
        val block = adapter.createBlock(json)
        name -> block
    } toMap
    
    jsonBlocks foreach {
      case (name, json) =>
        val typeName = json.get[String]("customType")
        val adapter = BlockFactory.adapterMap(typeName).asInstanceOf[BlockAdapter[DSARxBlock]]
        val block = blocksByName(name)
        adapter.setupBlock(block, json, blocksByName)
    }
    
    blocksByName foreach {
      case (name, block) => block.output subscribe testSub(name)
    }
  }
  
  private def testSub(name: String) = Subscriber[Value](
    (x: Value) => debug(name + ": " + x),
    (err: Throwable) => error(s"$name: $err", err),
    () => info(s"$name: done"))
}