package org.dsa.iot.ignition

import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.dsa.iot.dslink.node._
import org.dsa.iot.dslink.util.json.{ JsonArray, JsonObject }

import com.ignition._

/**
 * Class responsible for providing step adapters and building workflows from designer output.
 */
abstract class BlockFactory[S <: Step[T, R], T, R <: FlowRuntime] {

  def adapters: Iterable[StepAdapter[S, T, R]]

  protected lazy val adapterMap = adapters map (a => (a.typeName, a)) toMap

  protected val One = List("")
  protected val Two = List("0", "1")

  protected def flowFactory: SubFlowFactory[_, T, R]

  /**
   * Creates a flow from the json exported from the designer.
   */
  def fromDesigner(json: JsonObject): (/*SubFlow[T, R]*/Iterable[ConnectionSource[T, R]], Map[String, S]) = {
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
        name -> step
    }

    // connect steps
    blocks foreach {
      case (name, obj) =>
        val typeName = obj.get[String]("customType")
        val tgtAdapter = adapterMap(typeName)
        val tgtStep = steps(name)

        val connectedInputs = tgtAdapter.inputSuffixes.map { suffix =>
          Option(obj.get[JsonArray](s"input$suffix")) map (_.get[String](0))
        }.zipWithIndex collect {
          case (Some(str), index) => (str, index)
        }

        connectedInputs foreach {
          case (outStr, tgtIndex) =>
            val Array(_, srcName, srcOutput) = outStr.split('.')
            val srcStep = steps(srcName)
            val srcAdapter = adapterMap(blocks(srcName).get[String]("customType"))
            val srcOutSuffix = srcOutput.drop("output".length)
            val srcIndex = srcAdapter.outputSuffixes.zipWithIndex.find(t => t._1 == srcOutSuffix).get._2
            outs(srcStep)(srcIndex) --> ins(tgtStep)(tgtIndex)
        }
    }

    // find target points
//    val sourceSteps = steps.values.flatMap { step =>
//      for {
//        inbound <- ins(step).map(p => p.inbound) if inbound != null
//        srcStep = inbound.step if step != null
//      } yield srcStep
//    }.toSet
//
//    val outSteps = steps.values filterNot sourceSteps
//    val outPoints = outSteps flatMap outs
    val outPoints = steps.values flatMap outs

//    (flowFactory.instantiate(Nil, outPoints.toSeq), steps)
    (outPoints, steps)
  }

  /**
   * Returns a list of input ports for the step.
   */
  private def ins(step: Step[T, R]): Seq[ConnectionTarget[T, R]] = step match {
    case x if x.isInstanceOf[SingleInputStep[T, R]] => List(x.asInstanceOf[SingleInputStep[T, R]])
    case x if x.isInstanceOf[MultiInputStep[T, R]] => x.asInstanceOf[MultiInputStep[T, R]].in
    case _ => Nil
  }

  /**
   * Returns a list of output ports for the step.
   */
  private def outs(step: Step[T, R]): Seq[ConnectionSource[T, R]] = step match {
    case x if x.isInstanceOf[SingleOutputStep[T, R]] => List(x.asInstanceOf[SingleOutputStep[T, R]])
    case x if x.isInstanceOf[MultiOutputStep[T, R]] => x.asInstanceOf[MultiOutputStep[T, R]].out
    case _ => Nil
  }
}