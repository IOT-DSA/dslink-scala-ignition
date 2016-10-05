package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import org.dsa.iot.scala.Having

/**
 * Writes rows to a CSV file.
 */
class TextFileOutput(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def filename(name: String): TextFileOutput = this having (filename <~ name)
  def formats(values: (String, String)*): TextFileOutput = this having (formats <~ values)
  def separator(str: String): TextFileOutput = this having (separator <~ str)
  def withHeader(): TextFileOutput = this having (header <~ true)
  def noHeader(): TextFileOutput = this having (header <~ false)

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
   * Creates a new TextFileOutput with the "," as separator and header.
   */
  def apply()(implicit rt: SparkRuntime): TextFileOutput = new TextFileOutput separator "," withHeader
}