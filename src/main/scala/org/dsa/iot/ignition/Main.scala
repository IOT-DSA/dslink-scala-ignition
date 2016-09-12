package org.dsa.iot.ignition

import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.DataFrame
import org.dsa.iot.{ ActionHandler, DSAConnector, DSAHelper, RichActionResult, RichNode, RichNodeBuilder, RichValueType, createAction }
import org.dsa.iot.{ toList, valueToString }
import org.dsa.iot.dslink.node.Node
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.dslink.node.value.ValueType.STRING
import org.slf4j.LoggerFactory
import org.dsa.iot.dslink.node.actions.table.Table
import scala.util.control.NonFatal

object Main extends App {
  import Settings._

  private val log = LoggerFactory.getLogger(getClass)

  log.info("Command line: " + args.mkString(" "))

  lazy val connector = DSAConnector(args)
  lazy val connection = connector.start

  implicit def requester = connection.requester
  implicit def responder = connection.responder

  val root = connection.responderLink.getNodeManager.getSuperRoot
  initRoot(root)

  log.info("Application controller started")

  /* temporary commented out for testing 
  println("Press Ctrl+C to shut down")
  sys.addShutdownHook(shutdown)
  */
  println("\nPress ENTER to continue")
  Console.in.readLine
  shutdown

  private def shutdown() = {
    connector.stop
    sys.exit(0)
  }

  /* controller */

  private def initRoot(root: Node) = {
    root createChild "newFlow" display "New Flow" action addFlow build ()
    root.children.values filter isFlowNode foreach initFlowNode
    log.info("Node hierarchy initialized")
  }

  private def initFlowNode(node: Node) = {
    val name = node.getName

    node.setMetaData(new RxFlow(name))

    node createChild "listDataflowBlocks" display "List Blocks" action listBlocks build ()
    node createChild "updateDataflow" display "Update Flow" action updateFlow build ()
    node createChild "startFlow" display "(Re)start Flow" action startFlow build ()
    node createChild "stopFlow" display "Stop Flow" action stopFlow build ()
    node createChild "removeFlow" display "Remove Flow" action removeFlow build ()

    log.info(s"Flow node [$name] initialized")
  }

  private def rebuildFlow(node: Node) = {
    val frsp = DSAHelper invokeAndWait s"$dfPath/${node.getName}/$dfExportCmd"
    frsp foreach { rsp =>
      val json = rsp.getTable.getRows.get(0).getValues.get(0).getMap
      val flow = node.getMetaData[RxFlow]
      val wasRunning = flow.isRunning
      try {
        flow.update(json)
        flow.allBlocks foreach {
          case (name, block) =>
            val path = s"$dfPath/${node.getName}/$name/output"
            val stream = block.output map {
              case x: RichValue => x.self
              case x: Value     => x
              case x: DataFrame => org.dsa.iot.mapToValue(spark.dataFrameToTableData(x))
              case x: Table     => org.dsa.iot.mapToValue(tableToMap(x))
              case x            => org.dsa.iot.anyToValue(x)
            }
            stream subscribe (DSAHelper.set(path, _))
        }
      } catch {
        case NonFatal(e) => log.error(s"Error updating flow [${flow.name}]...", e)
      } finally {
        if (wasRunning)
          flow.restart
      }
    }
  }

  /* actions */

  lazy val listBlocks: ActionHandler = event => {
    val tbl = event.getTable
    RxBlockFactory.adapters foreach { sa => tbl.addRow(sa.makeRow) }
  }

  lazy val addFlow = createAction(
    parameters = STRING("name"),
    handler = event => {
      val parent = event.getNode.getParent
      val name = event.getParam[String]("name", !_.isEmpty, "Name cannot be empty").trim
      val flowNode = createFlowNode(parent, name)

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
      case rsp => log.info(s"Flow [$name] removed")
    }
  }

  lazy val startFlow: ActionHandler = event => {
    val node = event.getNode.getParent
    val flow = node.getMetaData[RxFlow]
    flow.restart
  }

  lazy val stopFlow: ActionHandler = event => {
    val node = event.getNode.getParent
    val flow = node.getMetaData[RxFlow]
    flow.shutdown
  }

  lazy val updateFlow: ActionHandler = event => {
    val node = event.getNode.getParent
    rebuildFlow(node)
  }
}