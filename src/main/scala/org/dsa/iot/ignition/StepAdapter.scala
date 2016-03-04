package org.dsa.iot.ignition

import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.dslink.node.value.Value
import org.dsa.iot.dslink.util.json.{ JsonArray, JsonObject }

import com.ignition.{ FlowRuntime, Step }

/**
 * A bridge between Ignition step and DGLux dataflow block.
 */
trait StepAdapter[S <: Step[T, R], T, R <: FlowRuntime] {

  /**
   * Block type name.
   */
  def typeName: String

  /**
   * Block category.
   */
  def category: String

  /**
   * Input suffixes.
   */
  def inputSuffixes: Iterable[String]

  /**
   * Output suffixes.
   */
  def outputSuffixes: Iterable[String]

  /**
   * Creates a block descriptor row to pass to the designer.
   */
  def makeRow: Row

  /**
   * Creates an ignition step from JSON returned by the designer.
   */
  def makeStep(json: JsonObject): S
}

/**
 * An abstract implementation of StepAdapter exposing Row from the name and parameter types.
 */
abstract class AbstractStepAdapter[S <: Step[T, R], T, R <: FlowRuntime](
    val typeName: String,
    val category: String,
    val inputSuffixes: Iterable[String],
    val outputSuffixes: Iterable[String],
    parameters: (String, String)*) extends StepAdapter[S, T, R] {

  def this(typeName: String, category: String, inputSuffixes: Iterable[String], outputSuffixes: Iterable[String],
           parameters: List[(String, String)]) = this(typeName, category, inputSuffixes, outputSuffixes, parameters: _*)

  private val jsonParams = {
    def param(pName: String, pType: String) = new JsonObject(s"""{"name" : "$pName", "type" : "$pType"}""")
    val arr = new JsonArray
    parameters foreach { case (pName, pType) => arr.add(param(pName, pType)) }
    inputSuffixes foreach { suffix => arr.add(param(s"input$suffix", "tabledata")) }
    outputSuffixes foreach { suffix => arr.add(param(s"output$suffix", "tabledata")) }
    arr
  }

  val makeRow = Row.make(new Value(typeName), new Value(jsonParams), new Value(category))
}