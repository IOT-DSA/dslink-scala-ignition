package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import com.ignition.frame.mllib.{ RegressionConfig, RegressionMethod }
import com.ignition.frame.mllib.RegressionMethod.RegressionMethod
import org.apache.spark.mllib.regression.GeneralizedLinearModel

import org.dsa.iot.scala.Having

/**
 * Computes the regression using MLLib library.
 */
class Regression(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def label(fld: String): Regression = this having (labelField <~ fld)
  def fields(flds: String*): Regression = this having (dataFields <~ flds)
  def groupBy(fields: String*): Regression = this having (groupBy <~ fields.toList)
  def method(md: RegressionMethod[_ <: GeneralizedLinearModel]): Regression = this having (method <~ md)
  def iterations(n: Int): Regression = this having (iterationCount <~ n)
  def step(size: Double): Regression = this having (stepSize <~ size)
  def intercept(allow: Boolean): Regression = this having (allowIntercept <~ allow)

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
   * Creates a new Regression instance with the LINEAR regression method, 100 iterations
   * with step of 0.1, no intercept and no grouping columns.
   */
  def apply()(implicit rt: SparkRuntime): Regression = {
    new Regression method RegressionMethod.LINEAR iterations 100 step 1.0 intercept false groupBy ()
  }
}