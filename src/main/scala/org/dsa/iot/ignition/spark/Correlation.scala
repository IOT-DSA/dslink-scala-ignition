package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import com.ignition.frame.mllib.CorrelationMethod
import com.ignition.frame.mllib.CorrelationMethod.CorrelationMethod

/**
 * Computes the correlation between data series using MLLib library.
 */
class Correlation(implicit rt: SparkRuntime) extends RxFrameTransformer {
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
   * Creates a new Correlation instance with the specified method and GROUP BY columns.
   */
  def apply(method: CorrelationMethod = CorrelationMethod.PEARSON,
            groupBy: List[String] = Nil)(implicit rt: SparkRuntime): Correlation = {
    val block = new Correlation
    block.method <~ method
    block.groupBy <~ groupBy
    block
  }
}