package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import org.dsa.iot.scala.Having

/**
 * Prints out the data frame data to the standard output.
 */
class DebugOutput(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def names(show: Boolean): DebugOutput = this having (showNames <~ show)
  def types(show: Boolean): DebugOutput = this having (showTypes <~ show)
  def title(str: String): DebugOutput = this having (title <~ Some(str))
  def noTitle(): DebugOutput = this having (title <~ None)
  def width(n: Int): DebugOutput = this having (maxWidth <~ Some(n))
  def unlimitedWidth(): DebugOutput = this having (maxWidth <~ None)

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
   * Creates a new DebugOutput instance with no title, unlimited width, names and no types.
   */
  def apply()(implicit rt: SparkRuntime): DebugOutput = {
    new DebugOutput noTitle () unlimitedWidth () names true types false
  }
}