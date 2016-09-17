package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime

/**
 * Prints out the data frame data to the standard output.
 */
class DebugOutput(implicit rt: SparkRuntime) extends RxFrameTransformer {
  val showNames = Port[Boolean]("showNames")
  val showTypes = Port[Boolean]("showTypes")
  val title = Port[Option[String]]("title")
  val maxWidth = Port[Option[Int]]("maxWidth")

  protected def compute =
    (showNames.in combineLatest showTypes.in combineLatest title.in combineLatest maxWidth.in) flatMap {
      case (((names, types), ttl), width) => doTransform(com.ignition.frame.DebugOutput(names, types, ttl, width))
    }
}

/**
 * Factory for [[DebugOutput]] instances.
 */
object DebugOutput {

  /**
   * Creates a new DebugOutput instance with the specified title and maximum width.
   */
  def apply(title: Option[String] = None, maxWidth: Option[Int] = None)(implicit rt: SparkRuntime): DebugOutput = {
    val block = new DebugOutput
    block.title <~ title
    block.maxWidth <~ maxWidth
    block
  }
}