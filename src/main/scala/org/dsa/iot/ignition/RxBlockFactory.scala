package org.dsa.iot.ignition

import scala.reflect.runtime.universe

import org.dsa.iot.dslink.util.json.JsonObject
import org.dsa.iot.rx.RxTransformer

/**
 * Adapter for RxTransformer block.
 */
abstract class TransformerAdapter[T, S <: RxTransformer[T, _]](
  name: String, category: String, params: ParamInfo*)(implicit cnv: Any => T)
    extends AbstractRxBlockAdapter[S](name, category, (params :+ ParamInfo.input): _*) {

  def setupBlock(block: S, json: JsonObject, blocks: DSABlockMap): Unit = {
    setupAttributes(block, json, blocks)
    connect(block.source, json, "input", blocks)
  }

  def setupAttributes(block: S, json: JsonObject, blocks: DSABlockMap): Unit
}

/**
 * Rx block adapter factory.
 */
object RxBlockFactory {

  lazy val adapters = listAdapters[core.CoreBlockFactory.type] ++ listAdapters[spark.SparkBlockFactory.type]

  lazy val adapterMap: Map[String, RxBlockAdapter[_ <: DSARxBlock]] = adapters.map(a => a.name -> a).toMap

  private def listAdapters[TT: universe.TypeTag] = listMemberModules[TT] collect {
    case a: RxBlockAdapter[_] => a.asInstanceOf[RxBlockAdapter[_ <: DSARxBlock]]
  }
}