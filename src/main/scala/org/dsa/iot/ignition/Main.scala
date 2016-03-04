package org.dsa.iot.ignition

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal

import org.apache.spark.streaming.Milliseconds
import org.dsa.iot.{ DSAConnector, DSAHelper, RichNode, RichNodeBuilder }
import org.dsa.iot.dslink.DSLinkHandler
import org.dsa.iot.dslink.node.Node
import org.dsa.iot.spark.DSAReceiver
import org.slf4j.LoggerFactory

import com.ignition.{ ExecutionException, SparkHelper }
import com.ignition.stream.{ DefaultSparkStreamingRuntime, StreamStepBatchProcessed, StreamStepDataListener }
import com.ignition.util.ConfigUtils

object FlowType extends Enumeration {
  type FlowType = Value
  val FRAME, STREAM = Value
}

object Main extends App {
  import Settings._
  import FlowType._
  import Actions._

  private val log = LoggerFactory.getLogger(getClass)

  lazy val connector = DSAConnector(args)
  lazy val connection = connector.start

  implicit def requester = connection.requester
  implicit def responder = connection.responder

  DSAReceiver.setRequester(requester)

  lazy val (runtime, previewRuntime) = {
    val streamCfg = ConfigUtils.getConfig("spark.streaming")
    val ms = streamCfg.getDuration("batch-duration", TimeUnit.MILLISECONDS)
    val duration = Milliseconds(ms)
    (new DefaultSparkStreamingRuntime(SparkHelper.sqlContext, duration, false),
      new DefaultSparkStreamingRuntime(SparkHelper.sqlContext, duration, true))
  }

  lazy val FLOW_TYPE = "flowType"

  private lazy val root = connection.responderLink.getNodeManager.getSuperRoot
  initRoot(root)

  log.info("Application controller started")

  /**
   * Initializes the root node.
   */
  private def initRoot(root: Node) = {
    val frames = root createChild "frames" display "Frames" build ()
    initFrameRoot(frames)

    val streams = root createChild "streams" display "Streams" build ()
    initStreamRoot(streams)
  }

  /**
   * Initializes the root node for frame flows.
   */
  private def initFrameRoot(parent: Node) = {
    parent createChild "newFrameFlow" display "New Frame Flow" action addFlow(FRAME) build ()
    parent.children.values filter (_.configurations.contains(FLOW_TYPE)) foreach initFlowNode
  }

  /**
   * Initializes the root node for stream flows.
   */
  private def initStreamRoot(parent: Node) = {
    parent createChild "newStreamFlow" display "New Stream Flow" action addFlow(STREAM) build ()
    parent.children.values filter (_.configurations.contains(FLOW_TYPE)) foreach initFlowNode
  }

  /**
   * Initializes a flow node.
   */
  def initFlowNode(node: Node) = {
    val name = node.getName
    val flowType = FlowType.withName(node.configurations(FLOW_TYPE).toString)

    val (listHandler, title, onImport) = if (flowType == FRAME)
      (listFrameBlocks, "Run Flow", runFrameFlow(false) _)
    else
      (listStreamBlocks, "Start Flow", startStreamFlow(false) _)

    node createChild "listDataflowBlocks" display "List Blocks" action listHandler build ()

    node createChild "importAndRun" display title action (event => onImport(node)) build ()

    node createChild "updateDataflow" display "Update Flow" action updateFlow build ()

    node createChild "removeFlow" display "Remove Flow" action removeFlow build ()

    log.debug(s"$flowType flow node $name initialized")
  }

  /**
   * Called on each frame flow update in the designer, recalculates all flow outputs.
   */
  def onFrameFlowUpdate = runFrameFlow(true) _

  /**
   * Imports and runs a frame flow.
   */
  private def runFrameFlow(previewMode: Boolean)(node: Node) =
    DSAHelper invokeAndWait s"$dfPath/${node.getName}/$dfExportCmd" onSuccess {
      case rsp =>
        val json = rsp.getTable.getRows.get(0).getValues.get(0).getMap
        try {
          val steps = FrameBlockFactory.fromDesigner(json)
          log.info(s"${steps.size} blocks imported for frame flow ${node.getName}")

          implicit val rt = runtime

          steps foreach {
            case (name, FlowBlock(step, adapter, json)) =>
              val results = step.evaluate
              results zip adapter.outputSuffixes foreach {
                case (data, suffix) =>
                  val path = s"$dfPath/${node.getName}/$name/output${suffix}"
                  DSAHelper.set(path, dataFrameToTableData(data))
              }
          }
        } catch {
          case e: ExecutionException => log.error(s"Error executing spark flow ${node.getName}: " + e.getMessage)
          case NonFatal(e)           => log.error(s"Error compiling flow ${node.getName}: " + e.getMessage)
        }
    }

  /**
   * Called on each stream flow update in the designer, re-registers all flow outputs and restarts streaming.
   */
  def onStreamFlowUpdate = startStreamFlow(true) _

  /**
   * Imports and runs a stream flow.
   */
  private def startStreamFlow(previewMode: Boolean)(node: Node) =
    DSAHelper invokeAndWait s"$dfPath/${node.getName}/$dfExportCmd" onSuccess {
      case rsp =>
        val json = rsp.getTable.getRows.get(0).getValues.get(0).getMap
        try {
          val steps = StreamBlockFactory.fromDesigner(json)
          log.info(s"${steps.size} blocks imported for stream flow ${node.getName}")

          implicit val rt = runtime

          if (rt.isRunning)
            rt.stop

          steps foreach {
            case (name, FlowBlock(step, adapter, json)) =>
              step.register
              step.addStreamDataListener(new StreamStepDataListener {
                override def onBatchProcessed(event: StreamStepBatchProcessed) = {
                  val suffix = adapter.outputSuffixes.toList(event.index)
                  val path = s"$dfPath/${node.getName}/$name/output${suffix}"
                  DSAHelper.set(path, rddToTableData(event.rows))
                }
              })
          }

          rt.start
        } catch {
          case e: ExecutionException => log.error(s"Error executing spark flow ${node.getName}: " + e.getMessage)
          case NonFatal(e)           => log.error(s"Error compiling flow ${node.getName}: " + e.getMessage)
        }
    }
}

class DummyDSLinkHandler extends DSLinkHandler