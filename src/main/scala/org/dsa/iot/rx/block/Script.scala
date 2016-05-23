package org.dsa.iot.rx.block

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.dsa.iot.dslink.node.value.Value
import org.mvel2.{ MVEL, ParserContext }

import com.ignition.script.ScriptFunctions

case class Script() extends DSARxBlock {
  val code = Port[Value]
  val input = Port[Value]

  protected def combineAttributes = code.in map (Seq(_))

  protected def combineInputs = Seq(input.in)

  protected def evaluator(attrs: Seq[Value]) = {
    val compiled = MVEL.compileExpression(attrs.head.toString, Filter.parserContext)

    (inputs: Seq[ValueStream]) => {
      inputs.head.map { value =>
        val args = Map[String, Any]("input" -> value).asJava
        val result = MVEL.executeExpression(compiled, ScriptFunctions, args)
        result.asInstanceOf[Value]
      }
    }
  }
}

/**
 * Provides parser context.
 */
object Script {

  lazy val parserContext = {
    val pctx = new ParserContext
    pctx.setStrictTypeEnforcement(true)
    ScriptFunctions.getClass.getDeclaredMethods foreach { method =>
      pctx.addImport(method.getName, method)
    }
    pctx.addInput("input", classOf[Value])
    pctx
  }
}