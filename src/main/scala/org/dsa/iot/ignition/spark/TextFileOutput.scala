package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime

/**
 * Writes rows to a CSV file.
 */
class TextFileOutput(implicit rt: SparkRuntime) extends RxFrameTransformer {
  val filename = Port[String]("filename")
  val formats = PortList[(String, String)]("formats")
  val separator = Port[String]("separator")
  val header = Port[Boolean]("header")

  protected def compute =
    (filename.in combineLatest formats.combinedIns combineLatest separator.in combineLatest header.in) flatMap {
      case (((fname, fmts), sep), hdr) => doTransform(com.ignition.frame.TextFileOutput(fname, fmts, sep, hdr))
    }
}

/**
 * Factory for [[TextFileOutput]] instances.
 */
object TextFileOutput {

  /**
   * Creates a new TextFileOutput with the specified separator and header flag.
   */
  def apply(separator: String = ",", header: Boolean = true)(implicit rt: SparkRuntime): TextFileOutput = {
    val block = new TextFileOutput
    block.separator <~ separator
    block.header <~ header
    block
  }
}