package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import com.ignition.frame.mllib.{ RegressionConfig, RegressionMethod }
import com.ignition.frame.mllib.RegressionMethod.RegressionMethod
import org.apache.spark.mllib.regression.GeneralizedLinearModel

/**
 * Computes the regression using MLLib library.
 */
class Regression(implicit rt: SparkRuntime) extends RxFrameTransformer {
  val labelField = Port[String]("labelField")
  val dataFields = PortList[String]("dataFields")
  val groupBy = Port[List[String]]("groupBy")
  val method = Port[RegressionMethod[_ <: GeneralizedLinearModel]]("method")
  val iterationCount = Port[Int]("iterations")
  val stepSize = Port[Double]("step")
  val allowIntercept = Port[Boolean]("intercept")

  protected def compute = (labelField.in combineLatest dataFields.combinedIns
    combineLatest groupBy.in combineLatest method.in combineLatest iterationCount.in
    combineLatest stepSize.in combineLatest allowIntercept.in) flatMap {
      case ((((((label, data), grp), method), iters), step), icept) =>
        val config = RegressionConfig(method, iters, step, icept)
        doTransform(com.ignition.frame.mllib.Regression(label, data, grp, config))
    }
}

/**
 * Factory for [[Regression]] instances.
 */
object Regression {

  /**
   * Creates a new Regression instance with the specified regression method, iteration count, step
   * size, intercept flag and GROUP BY columns.
   */
  def apply(regressionMethod: RegressionMethod[_ <: GeneralizedLinearModel] = RegressionMethod.LINEAR,
            iterationCount: Int = 100, stepSize: Double = 1.0, allowIntercept: Boolean = false,
            groupBy: List[String] = Nil)(implicit rt: SparkRuntime): Regression = {
    val block = new Regression
    block.method <~ regressionMethod
    block.iterationCount <~ iterationCount
    block.stepSize <~ stepSize
    block.allowIntercept <~ allowIntercept
    block.groupBy <~ groupBy
    block
  }
}