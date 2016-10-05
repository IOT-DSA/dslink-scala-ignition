package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import com.ignition.frame.mllib.CorrelationMethod
import com.ignition.frame.mllib.CorrelationMethod.CorrelationMethod

import org.dsa.iot.scala.Having

/**
 * Computes the correlation between data series using MLLib library.
 */
class Correlation(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def fields(cols: String*): Correlation = this having (dataFields <~ cols)
  def groupBy(fields: String*): Correlation = this having (groupBy <~ fields.toList)
  def method(mtd: CorrelationMethod): Correlation = this having (method <~ mtd)

  val dataFields = PortList[String]("fields")
  val groupBy = Port[List[String]]("groupBy")
  val method = Port[CorrelationMethod]("method")

  protected def compute = dataFields.combinedIns combineLatest groupBy.in combineLatest method.in flatMap {
    case ((fields, grp), method) => doTransform(com.ignition.frame.mllib.Correlation(fields, grp, method))
  }
}

/**
 * Factory for [[Correlation]] instances.
 */
object Correlation {

  /**
   * Creates a new Correlation instance.
   */
  def apply()(implicit rt: SparkRuntime): Correlation = {
    new Correlation method CorrelationMethod.PEARSON groupBy ()
  }
}