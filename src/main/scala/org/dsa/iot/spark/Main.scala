package org.dsa.iot.spark

import java.util.concurrent.{ Executors, TimeUnit }

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.{ Duration, Milliseconds }
import org.dsa.iot.{ DSAConnector, DSAHelper, RichNode, RichNodeBuilder }
import org.dsa.iot.dslink.DSLinkHandler
import org.dsa.iot.dslink.node.Node
import org.dsa.iot.spark.DSAReceiver
import org.slf4j.LoggerFactory

import com.ignition.{ SparkHelper, frame, stream }
import com.ignition.util.ConfigUtils

/**
 * Available Ignition flow types.
 */
object FlowType extends Enumeration {
  type FlowType = Value
  val FRAME, STREAM = Value
}

/**
 * The main application controller.
 */
object Main extends App {
  import Settings._
  import FlowType._
  import Actions._

  /* type aliases */
  type FrameFlowBlock = FlowBlock[frame.FrameStep, DataFrame, frame.SparkRuntime]
  type FrameFlowModel = Map[String, FrameFlowBlock]
  type StreamFlowBlock = FlowBlock[stream.StreamStep, stream.DataStream, stream.SparkStreamingRuntime]
  type StreamFlowModel = Map[String, StreamFlowBlock]

  private val log = LoggerFactory.getLogger(getClass)

  lazy val connector = DSAConnector(args)
  lazy val connection = connector.start

  implicit def requester = connection.requester
  implicit def responder = connection.responder

  DSAReceiver.setRequester(requester)

  // workaround for "spark.sql.execution.id is already set" problem
  lazy val executorService = Executors.newFixedThreadPool(4)
  implicit def ec = ExecutionContext.fromExecutorService(executorService)

  lazy val (runtime, previewRuntime) = {
    val streamCfg = ConfigUtils.getConfig("spark.streaming")
    val ms = streamCfg.getDuration("batch-duration", TimeUnit.MILLISECONDS)
    val duration = Milliseconds(ms)
    (createRuntime(duration, false), createRuntime(duration, true))
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

    if (flowType == FRAME) {
      node createChild "listDataflowBlocks" display "List Blocks" action listFrameBlocks build ()
      node createChild "runFlow" display "Run Flow" action (_ => runFrameFlow(false)(node)) build ()
    } else if (flowType == STREAM) {
      node createChild "listDataflowBlocks" display "List Blocks" action listStreamBlocks build ()
      node createChild "startFlow" display "Start Flow" action (_ => onStreamFlowUpdate(node)) build ()
      node createChild "stopFlow" display "Stop Streaming" action (_ => stopStreamFlow) build ()
    }

    node createChild "updateDataflow" display "Update Flow" action updateFlow build ()

    node createChild "removeFlow" display "Remove Flow" action removeFlow build ()

    log.debug(s"$flowType flow node $name initialized")
  }

  /**
   * Called on each frame flow update in the designer. Rebuilds and runs the workflow.
   */
  def onFrameFlowUpdate(node: Node) = {
    val frsp = DSAHelper invokeAndWait s"$dfPath/${node.getName}/$dfExportCmd"
    frsp foreach { rsp =>
      val json = rsp.getTable.getRows.get(0).getValues.get(0).getMap
      Try(FrameBlockFactory.fromDesigner(json)) match {
        case Success(steps) =>
          log.debug(s"${steps.size} blocks imported for frame flow [${node.getName}]")
          node.setMetaData(steps)
          runFrameFlow(true)(node)
        case Failure(e) => log.warn(s"Error compiling frame flow [${node.getName}]: " + e.getMessage)
      }
    }
  }

  /**
   * Imports and runs a frame flow.
   */
  def runFrameFlow(previewMode: Boolean)(node: Node): Unit = {

    implicit val rt = if (previewMode) previewRuntime else runtime

    def evaluateModel(steps: FrameFlowModel) = steps foreach {
      case (name, FlowBlock(step, adapter, _)) => try {
        val results = step.evaluate
        results zip adapter.outputSuffixes foreach {
          case (data, suffix) =>
            val path = s"$dfPath/${node.getName}/$name/output${suffix}"
            DSAHelper.set(path, dataFrameToTableData(data))
        }
      } catch {
        case NonFatal(e) => 
          log.error(s"Error evaluating step $name in frame flow [${node.getName}]: " + e.getMessage)
          e.printStackTrace
      }
    }

    val model = node.getMetaData[FrameFlowModel]
    if (model != null)
      evaluateModel(model)
    else
      log.warn(s"No valid model for frame flow [${node.getName}]")
  }

  /**
   * Called on each stream flow update in the designer, re-registers all flow outputs and restarts streaming.
   */
  def onStreamFlowUpdate(node: Node) = {
    val frsp = DSAHelper invokeAndWait s"$dfPath/${node.getName}/$dfExportCmd"
    frsp foreach { rsp =>
      val json = rsp.getTable.getRows.get(0).getValues.get(0).getMap
      Try(StreamBlockFactory.fromDesigner(json)) match {
        case Success(steps) =>
          log.debug(s"${steps.size} blocks imported for stream flow [${node.getName}]")
          node.setMetaData(steps)
          startStreamFlow(true)(node)
        case Failure(e) => log.warn(s"Error compiling stream flow [${node.getName}]: " + e.getMessage)
      }
    }
  }

  /**
   * (Re)starts a stream flow.
   */
  def startStreamFlow(previewMode: Boolean)(node: Node) = {
    implicit val rt = runtime

    if (rt.isRunning)
      rt.stop

    rt.unregisterAll

    def evaluateModel(steps: StreamFlowModel) = {
      steps foreach {
        case (name, FlowBlock(step, adapter, _)) => try {
          step.register
          step.addStreamDataListener(new stream.StreamStepDataListener {
            override def onBatchProcessed(event: stream.StreamStepBatchProcessed) = {
              val suffix = adapter.outputSuffixes.toList(event.index)
              val path = s"$dfPath/${node.getName}/$name/output${suffix}"
              DSAHelper.set(path, rddToTableData(event.rows))
            }
          })
        } catch {
          case NonFatal(e) => log.error(s"Error evaluating step $name in stream flow [${node.getName}]: " + e.getMessage)
        }
      }
      rt.start
    }

    val model = node.getMetaData[StreamFlowModel]
    if (model != null)
      evaluateModel(model)
    else
      log.warn(s"No valid model for stream flow [${node.getName}]")
  }

  /**
   * Stops the streaming.
   */
  def stopStreamFlow() = {
    implicit val rt = runtime

    if (rt.isRunning)
      rt.stop
  }

  /**
   * Creates a new flow runtime.
   */
  private def createRuntime(duration: Duration, preview: Boolean) =
    new stream.DefaultSparkStreamingRuntime(SparkHelper.sqlContext, duration, preview)
}

/**
 * Dummy DSLink handler to be put in dslink.json.
 */
class DummyDSLinkHandler extends DSLinkHandler