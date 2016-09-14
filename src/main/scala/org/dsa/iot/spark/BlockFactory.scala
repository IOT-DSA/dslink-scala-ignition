package org.dsa.iot.spark

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.reflect.runtime.universe

import org.dsa.iot.dslink.util.json.{ JsonArray, JsonObject }

import com.ignition.{ FlowRuntime, Step, SubFlowFactory, ins, outs }

/**
 * Flow block information, which includes the ignition step, step adapter and the source json.
 */
case class FlowBlock[S <: Step[T, R], T, R <: FlowRuntime](step: S, adapter: StepAdapter[S, T, R], json: JsonObject)

/**
 * Class responsible for providing step adapters and building workflows from designer output.
 */
abstract class BlockFactory[S <: Step[T, R], T, R <: FlowRuntime] {

  def adapters: Iterable[StepAdapter[S, T, R]]

  protected def buildAdapterList[TT: universe.TypeTag] = {
    val m = universe.runtimeMirror(getClass.getClassLoader)
    universe.typeOf[TT].declarations filter (_.isModule) map { obj =>
      m.reflectModule(obj.asModule).instance
    } collect {
      case m: StepAdapter[_, _, _] => m.asInstanceOf[StepAdapter[S, T, R]]
    }
  }

  protected lazy val adapterMap = adapters map (a => (a.typeName, a)) toMap

  protected val One = List("")
  protected val Two = List("0", "1")

  protected def flowFactory: SubFlowFactory[_, T, R]

  /**
   * Creates a flow from the json exported from the designer.
   */
  def fromDesigner(json: JsonObject): Map[String, FlowBlock[S, T, R]] = {
    // collect blocks
    val blocks = json.getMap.asScala.toMap collect {
      case (name, obj: JsonObject) => name -> obj
    }

    // create ignition steps
    val steps = blocks map {
      case (name, obj) =>
        val typeName = obj.get[String]("customType")
        val adapter = adapterMap(typeName)
        val step = adapter.makeStep(obj)
        name -> FlowBlock(step, adapter, obj)
    }

    // connect steps
    blocks foreach {
      case (name, obj) =>
        val typeName = obj.get[String]("customType")
        val tgtAdapter = adapterMap(typeName)
        val tgtStep = steps(name).step

        val connectedInputs = tgtAdapter.inputSuffixes.map { suffix =>
          Option(obj.get[JsonArray](s"input$suffix")) map (_.get[String](0))
        }.zipWithIndex collect {
          case (Some(str), index) => (str, index)
        }

        connectedInputs foreach {
          case (outStr, tgtIndex) =>
            val Array(_, srcName, srcOutput) = outStr.split('.')
            val srcStep = steps(srcName).step
            val srcAdapter = adapterMap(blocks(srcName).get[String]("customType"))
            val srcOutSuffix = srcOutput.drop("output".length)
            val srcIndex = srcAdapter.outputSuffixes.zipWithIndex.find(t => t._1 == srcOutSuffix).get._2
            outs(srcStep)(srcIndex) --> ins(tgtStep)(tgtIndex)
        }
    }

    steps
  }
}