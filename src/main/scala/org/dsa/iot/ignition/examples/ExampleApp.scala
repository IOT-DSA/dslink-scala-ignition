package org.dsa.iot.ignition.examples

import org.dsa.iot.spark._
import org.dsa.iot.dslink.node.actions._
import org.dsa.iot.dslink.node._
import org.dsa.iot.dslink.util.handler._
import org.dsa.iot.dslink.node.value._
import org.dsa.iot.dslink.node.actions.table._
import scala.io.Source
import com.ignition._
import org.json4s.jackson.JsonMethods._

object ExampleApp extends App {
  import org.dsa.iot.ignition.Main._

  private lazy val dfJson = Source.fromInputStream(getClass.getResourceAsStream("/dataflow.json")).mkString
  private lazy val sfJson = Source.fromInputStream(getClass.getResourceAsStream("/streamflow.json")).mkString

  private val root = connection.responderLink.getNodeManager.getSuperRoot

  private val dfNode = buildDataflowNode
  private val sfNode = buildStreamflowNode

  private def buildDataflowNode = {
    val builder = root.createChild("New DataFlow")

    val action = new Action(Permission.READ, new Handler[ActionResult] {
      def handle(event: ActionResult) = {
        val name = event.getParameter("name")
        val text = event.getParameter("text")

        val flow = root.createChild(name.getString).build
        flow.createChild("name").setValueType(ValueType.STRING).setValue(name).build
        flow.createChild("text").setValueType(ValueType.STRING).setValue(text).build

        val run = new Action(Permission.READ, new Handler[ActionResult] {
          def handle(event: ActionResult) = {
            val text = event.getNode.getParent.getChild("text").getValue.getString
            val f = frame.FrameFlow.fromJson(parse(text.replace("$" + "{flow}", name.getString)))
            frame.Main.runFrameFlow(f)
          }
        })
        flow.createChild("Run").setAction(run).build
      }
    })
    action.addParameter(new Parameter("name", ValueType.STRING))
    action.addParameter(new Parameter("text", ValueType.STRING, new Value(dfJson)))

    builder.setAction(action)
    builder.build
  }

  private def buildStreamflowNode = {
    val builder = root.createChild("New StreamFlow")

    val action = new Action(Permission.READ, new Handler[ActionResult] {
      def handle(event: ActionResult) = {
        val name = event.getParameter("name")
        val text = event.getParameter("text")

        val flow = root.createChild(name.getString).build
        flow.createChild("name").setValueType(ValueType.STRING).setValue(name).build
        flow.createChild("text").setValueType(ValueType.STRING).setValue(text).build
        flow.createChild("started").setValueType(ValueType.BOOL).setValue(new Value(false)).build

        val start = new Action(Permission.READ, new Handler[ActionResult] {
          def handle(event: ActionResult) = {
            val text = event.getNode.getParent.getChild("text").getValue.getString
            val f = stream.StreamFlow.fromJson(parse(text.replace("$" + "{flow}", name.getString)))
            event.getNode.getParent.getChild("started").setValue(new Value(true))
            stream.Main.startStreamFlow(f)
          }
        })
        flow.createChild("Start").setAction(start).build
      }
    })
    action.addParameter(new Parameter("name", ValueType.STRING))
    action.addParameter(new Parameter("text", ValueType.STRING, new Value(sfJson)))

    builder.setAction(action)
    builder.build
  }
  
  case class Abc(name: String, age: Int)
  
  {
    import org.apache.spark._
    import org.apache.spark.sql._
    
    val sc = new SparkContext()
    val sql = new SQLContext(sc)
    import sql.implicits._
    
    val rdd = sc.parallelize(Seq(Abc("a", 5), Abc("b", 3)))
    val df = rdd.toDF
  }
}