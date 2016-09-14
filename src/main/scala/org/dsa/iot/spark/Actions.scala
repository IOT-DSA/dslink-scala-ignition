package org.dsa.iot.spark

import scala.concurrent.ExecutionContext.Implicits.global
import org.dsa.iot.scala._
import org.dsa.iot.dslink.node.value.ValueType.STRING
import org.slf4j.LoggerFactory
import com.ignition.FlowRuntime

object Actions {
  type StepAdapterList = Iterable[StepAdapter[_, _, _ <: FlowRuntime]]

  import Settings._
  import FlowType._
  import Main._

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Lists the available workflow blocks.
   */
  private def listBlockHandler(adapters: StepAdapterList): ActionHandler = event => {
    val tbl = event.getTable
    adapters foreach { sa => tbl.addRow(sa.makeRow) }
  }

  val listFrameBlocks = listBlockHandler(FrameBlockFactory.adapters)

  val listStreamBlocks = listBlockHandler(StreamBlockFactory.adapters)

  /**
   * Creates a new Frame or Stream flow.
   */
  def addFlow(flowType: FlowType) = createAction(
    parameters = STRING("name"),
    handler = event => {
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
  def removeFlow = createAction(event => {
    val node = event.getNode.getParent
    val name = node.getName

    node.delete

    DSAHelper invokeAndWait s"$dfPath/$name/$dfDeleteCmd" onSuccess {
      case rsp => log.info(s"Flow ${node.getName} removed")
    }
  })

  /**
   * Updates the flow.
   */
  def updateFlow = createAction(event => {
    val node = event.getNode.getParent
    val flowType = FlowType.withName(node.configurations(FLOW_TYPE).toString)
    log.debug(s"$flowType flow [${node.getName}] updated in the designer")

    flowType match {
      case FRAME  => onFrameFlowUpdate(node)
      case STREAM => onStreamFlowUpdate(node)
    }
  })
}