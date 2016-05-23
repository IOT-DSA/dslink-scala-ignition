package org.dsa.iot.rx

import scala.concurrent.ExecutionContext.Implicits.global

import org.dsa.iot.{ ActionHandler, DSAConnector, DSAHelper, RichActionResult, RichNode, RichNodeBuilder, RichValueType, createAction }
import org.dsa.iot.{ toList, valueToString }
import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.value.ValueType.STRING
import org.dsa.iot.ignition.Settings
import org.slf4j.LoggerFactory

object NodeTypes {
  val NODE_TYPE = "nodeType"

  val FLOW = "flow"
}

object RxMain extends App {
  import org.dsa.iot.ignition.Settings._
  import NodeTypes._

  private val log = LoggerFactory.getLogger(getClass)

  log.info("Command line: " + args.mkString(" "))

  lazy val connector = DSAConnector(args)
  lazy val connection = connector.start

  implicit def requester = connection.requester
  implicit def responder = connection.responder

  val root = connection.responderLink.getNodeManager.getSuperRoot
  initRoot(root)

  log.info("Application controller started")

  /* controller */

  private def initRoot(root: Node) = {
    root createChild "newFlow" display "New Flow" action addFlow build ()
    root.children.values filter (_.configurations.get(NODE_TYPE) == Some(FLOW)) foreach initFlowNode
    log.info("Node hierarchy initialized")
  }

  private def initFlowNode(node: Node) = {
    val name = node.getName

    node.setMetaData(new RxFlow)

    node createChild "listDataflowBlocks" display "List Blocks" action listBlocks build ()
    node createChild "updateDataflow" display "(Re)start Flow" action updateFlow build ()
    node createChild "stopFlow" display "Stop Flow" action stopFlow build ()
    node createChild "removeFlow" display "Remove Flow" action removeFlow build ()

    log.info(s"Flow node [$name] initialized")
  }

  /* actions */

  lazy val addFlow = createAction(
    parameters = STRING("name"),
    handler = event => {
      val parent = event.getNode.getParent
      val name = event.getParam[String]("name", !_.isEmpty, "Name cannot be empty").trim
      val flowNode = parent createChild name config (dfDesignerKey -> s"$dfPath/$name", NODE_TYPE -> FLOW) build ()

      initFlowNode(flowNode)

      DSAHelper invoke (dfCreatePath, "name" -> name)

      log.info(s"Flow [$name] created")
    })

  lazy val removeFlow: ActionHandler = event => {
    val node = event.getNode.getParent
    val name = node.getName

    val flow = node.getMetaData[RxFlow]
    flow.shutdown

    node.delete

    DSAHelper invokeAndWait s"$dfPath/$name/$dfDeleteCmd" onSuccess {
      case rsp => log.info(s"Flow [${node.getName}] removed")
    }
  }

  lazy val stopFlow: ActionHandler = event => {
    val node = event.getNode.getParent
    val name = node.getName

    val flow = node.getMetaData[RxFlow]
    flow.shutdown
  }

  lazy val listBlocks: ActionHandler = event => {
    val tbl = event.getTable
    block.BlockFactory.adapters foreach { sa => tbl.addRow(sa.makeRow) }
  }

  lazy val updateFlow: ActionHandler = event => {
    val node = event.getNode.getParent

    val frsp = DSAHelper invokeAndWait s"$dfPath/${node.getName}/$dfExportCmd"
    frsp foreach { rsp =>
      val json = rsp.getTable.getRows.get(0).getValues.get(0).getMap
      val flow = node.getMetaData[RxFlow]
      flow.update(json)
      flow.blocksByName foreach {
        case (name, block) =>
          val path = s"$dfPath/${node.getName}/$name/output"
          block.output subscribe (DSAHelper.set(path, _))
      }
    }
  }
}