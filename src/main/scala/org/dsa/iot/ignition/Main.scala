package org.dsa.iot.ignition

import scala.concurrent.ExecutionContext.Implicits.global
import org.dsa.iot._
import org.dsa.iot.spark.DSAReceiver
import org.dsa.iot.dslink.DSLinkHandler
import org.slf4j.LoggerFactory
import org.dsa.iot.dslink.node.actions.ActionResult
import org.dsa.iot.dslink.node.value.ValueType._
import org.dsa.iot.dslink.node._
import com.ignition.FlowRuntime
import org.dsa.iot.dslink.util.json.JsonObject
import com.ignition.frame.FrameFlow
import com.ignition.stream.StreamFlow
import com.ignition.StepListener
import org.apache.spark.sql.DataFrame
import com.ignition.frame.SparkRuntime
import com.ignition.AfterStepComputed
import com.ignition.frame.FrameFlowStarted
import com.ignition.frame.FrameFlowComplete
import com.ignition.frame.FrameFlowListener
import com.ignition.stream.StreamStepDataListener
import com.ignition.stream.StreamStepBatchProcessed
import com.ignition.stream.StreamFlowListener
import com.ignition.stream.StreamFlowStarted
import com.ignition.stream.StreamFlowTerminated
import org.apache.spark.sql.SQLContext

object FlowType extends Enumeration {
  type FlowType = Value
  val FRAME, STREAM = Value
}

object Main extends App {
  import Settings._
  import FlowType._

  private val log = LoggerFactory.getLogger(getClass)

  lazy val connector = DSAConnector(args)
  lazy val connection = connector.start

  implicit def requester = connection.requester
  implicit def responder = connection.responder

  DSAReceiver.setRequester(requester)

  private lazy val root = connection.responderLink.getNodeManager.getSuperRoot
  initRoot(root)

  log.info("Application controller started")

  /**
   * Initializes the root node.
   */
  private def initRoot(root: Node) = {
    root createChild "newFrameFlow" display "New Frame Flow" action addFlow(FRAME) build ()
    root createChild "newStreamFlow" display "New Stream Flow" action addFlow(STREAM) build ()

    root.children.values filter (_.configurations.contains("flowType")) foreach initFlowNode
  }

  /**
   * Initializes a flow node.
   */
  private def initFlowNode(node: Node) = {
    val name = node.getName
    val flowType = FlowType.withName(node.configurations("flowType").toString)

    val (handler, title, onImport) = if (flowType == FRAME)
      (LIST_FRAME_BLOCKS, "Run Flow", onImportFrameFlow _)
    else
      (LIST_STREAM_BLOCKS, "Start Flow", onImportStreamFlow _)

    node createChild "listDataflowBlocks" display "List Blocks" action handler build ()
    node createChild "importAndRun" display title action (event => {
      DSAHelper invokeAndWait s"$dfPath/$name/$dfExportCmd" onSuccess {
        case rsp =>
          val json = rsp.getTable.getRows.get(0).getValues.get(0).getMap
          onImport(json, node)
      }
    }) build ()

    node createChild "removeFlow" display "Remove Flow" action removeFlow build ()
  }

  private def onImportFrameFlow(json: JsonObject, parent: Node) = {
    val (outPoints, steps) = FrameBlockFactory.fromDesigner(json)
    val flow = FrameFlow(outPoints)
    log.info("flow imported: " + flow)

    steps foreach {
      case (name, step) =>
        val node = parent createChild name build ()
        val outputNode = node createChild "output" valueType DYNAMIC build ()
        step.addStepListener(new StepListener[DataFrame, SparkRuntime] {
          override def onAfterStepComputed(event: AfterStepComputed[DataFrame, SparkRuntime]) = {
            val baos = new java.io.ByteArrayOutputStream
            Console.withOut(baos) { event.value.show }
            log.info(s"Block [$name] output ${event.index} computed:\n$baos")
            outputNode.setValue(anyToValue(event.value.collect.toList.map(_.toSeq.toList)))
          }
        })
    }

    flow.addFrameFlowListener(new FrameFlowListener {
      def onFrameFlowStarted(event: FrameFlowStarted) = log.info("Frame flow started")
      def onFrameFlowComplete(event: FrameFlowComplete) = log.info("Frame flow complete")
    })

    com.ignition.frame.Main.runFrameFlow(flow)
  }

  private def onImportStreamFlow(json: JsonObject, parent: Node) = {
    val (outPoints, steps) = StreamBlockFactory.fromDesigner(json)
    val flow = StreamFlow(outPoints)
    log.info("flow imported: " + flow)

    steps foreach {
      case (name, step) =>
        val node = parent createChild name build ()
        val outputNode = node createChild "output" valueType ARRAY build ()
        step.addStreamDataListener(new StreamStepDataListener {
          override def onBatchProcessed(event: StreamStepBatchProcessed) = {
            val rows = event.rows
            if (!rows.isEmpty) {
              val schema = rows.first.schema
              val cqlCtx = new SQLContext(rows.context)
              val df = cqlCtx.createDataFrame(rows, schema)
              val baos = new java.io.ByteArrayOutputStream
              Console.withOut(baos) { df.show }
              log.info(s"Block [$name] output computed at ${event.time}:\n$baos")
              outputNode.setValue(anyToValue(df.collect.toList.map(_.toSeq.toList)))
            }
          }
        })
    }

//    val (flowId, frsp) = com.ignition.stream.Main.startStreamFlow(flow)
//    log.info(s"Stream flow started with ID $flowId")
//    parent.setAttribute("flowId", flowId.toString)
//
//    frsp onComplete { _ =>
//      parent.removeChild("stopFlow")
//    }

//    parent createChild "stopFlow" display "Stop Flow" action (event => {
//      val flowId = event.getNode.getParent.attributes("flowId").toString
//      log.info(s"Terminating stream flow $flowId")
//      com.ignition.stream.Main.stopStreamFlow(java.util.UUID.fromString(flowId))
//    }) build
  }

  /* actions */

  def listBlockHandler(adapters: Iterable[StepAdapter[_, _, _ <: FlowRuntime]]): ActionHandler = event => {
    val tbl = event.getTable
    adapters foreach { sa => tbl.addRow(sa.makeRow) }
  }

  val LIST_FRAME_BLOCKS = listBlockHandler(FrameBlockFactory.adapters)

  val LIST_STREAM_BLOCKS = listBlockHandler(StreamBlockFactory.adapters)

  /**
   * Creates a new Frame or Stream flow.
   */
  def addFlow(flowType: FlowType) = createAction(
    parameters = STRING("name"),
    handler = (event: ActionResult) => {
      val parent = event.getNode.getParent
      val name = event.getParam[String]("name", !_.isEmpty, "Name cannot be empty").trim
      val flowNode = parent createChild name config (dfDesignerKey -> s"$dfPath/$name", "flowType" -> flowType) build ()

      initFlowNode(flowNode)

      DSAHelper invoke (dfCreatePath, "name" -> name)

      log.info(s"$flowType flow $name created")
    })

  /**
   * Removes the flow.
   */
  def removeFlow = createAction((event: ActionResult) => {
    val node = event.getNode.getParent
    val name = node.getName

    node.delete

    DSAHelper invokeAndWait s"$dfPath/$name/$dfDeleteCmd" onSuccess {
      case rsp => log.info(s"Flow ${node.getName} removed")
    }
  })
}

class DummyDSLinkHandler extends DSLinkHandler