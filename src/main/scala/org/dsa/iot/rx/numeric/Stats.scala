package org.dsa.iot.rx.numeric

/**
 * A class for tracking the statistics of a set of numbers (count, mean and variance) in a
 * numerically robust way. Includes support for merging two Stats objects. Based on Welford
 * and Chan's [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance algorithms]]
 * for running variance.
 *
 * @constructor Initialize the Stats with the given values.
 */
case class Stats(count: Long, mean: Double, varnum: Double, min: Double, max: Double) {
  if (count == 0)
    require(mean.isNaN && varnum.isNaN && min.isNaN && max.isNaN)

  /**
   * Merges this Stats with another instance, producing the cumulative stats.
   */
  def merge(that: Stats): Stats =
    if (this.count == 0) that
    else if (that.count == 0) this
    else {
      val delta = that.mean - this.mean
      val newMean = if (that.count * 10 < this.count)
        mean + (delta * that.count) / (this.count + that.count)
      else if (this.count * 10 < that.count)
        that.mean - (delta * this.count) / (this.count + that.count)
      else
        (this.mean * this.count + that.mean * that.count) / (this.count + that.count)
      val newVarnum = this.varnum + that.varnum +
        (delta * delta * this.count * that.count) / (this.count + that.count)
      val newCount = this.count + that.count
      val newMax = math.max(this.max, that.max)
      val newMin = math.min(this.min, that.min)
      Stats(newCount, newMean, newVarnum, newMin, newMax)
    }

  /**
   * Merges this Stats with another instance, producing the cumulative stats.
   */
  def ++(that: Stats) = merge(that)

  /**
   * Merges this Stats with a single value, producing the cumulative stats.
   */
  def merge(value: Double): Stats = merge(Stats.from(value))

  /**
   * Merges this Stats with a single value, producing the cumulative stats.
   */
  def +(value: Double) = merge(Stats.from(value))

  /**
   * Merges this Stats with a set of values, producing the cumulative stats.
   */
  def merge(values: TraversableOnce[Double]): Stats = merge(Stats.from(values))

  /**
   * Merges this Stats with a set of values, producing the cumulative stats.
   */
  def ++(values: TraversableOnce[Double]) = merge(values)

  /**
   * Returns the sum of the distribution.
   */
  def sum: Double = count * mean

  /**
   * Returns the variance of the distribution.
   */
  def variance: Double = if (count == 0) Double.NaN else varnum / count

  /**
   * Return the sample variance, which corrects for bias in estimating the variance
   * by dividing by N-1 instead of N.
   */
  def sampleVariance = if (count <= 1) Double.NaN else varnum / (count - 1)

  /**
   * Returns the standard deviation of the values.
   */
  def stdev = math.sqrt(variance)

  /**
   * Returns the coefficient of variation.
   */
  def covar = if (mean == 0) Double.NaN else stdev / mean

  /**
   * Return the sample standard deviation of the values, which corrects for bias
   * in estimating the variance by dividing by N-1 instead of N.
   */
  def sampleStdev = math.sqrt(sampleVariance)

  /**
   * Returns a string representation of this distribution.
   */
  override def toString: String =
    "(count: %d, mean: %f, stdev: %f, max: %f, min: %f)".format(count, mean, stdev, max, min)

  /**
   * Overrides to account for empty stats with NaN values.
   */
  override def equals(that: Any): Boolean = that match {
    case x: Stats if this eq x => true
    case Stats(0, _, _, _, _)  => this.count == 0
    case Stats(count, mean, varnum, min, max) => count == this.count && mean == this.mean &&
      varnum == this.varnum && min == this.min && max == this.max
    case _ => false
  }
}

/**
 * Stats companion object.
 */
object Stats {

  /**
   * A singleton, empty Stats instance.
   */
  val empty = Stats(0, Double.NaN, Double.NaN, Double.NaN, Double.NaN)

  /**
   * Creates a Stats instance from a set of values.
   */
  def from(values: TraversableOnce[Double]): Stats =
    if (values.isEmpty) empty
    else {
      var mean = 0.0
      var varnum = 0.0
      var count = 0L
      var maxValue = Double.NegativeInfinity
      var minValue = Double.PositiveInfinity

      values foreach { value =>
        val delta = value - mean
        count += 1
        mean += delta / count
        varnum += delta * (value - mean)
        maxValue = math.max(maxValue, value)
        minValue = math.min(minValue, value)
      }

      Stats(count, mean, varnum, minValue, maxValue)
    }

  /**
   * Creates a Stats instance from a single value.
   */
  def from(value: Double): Stats = Stats(1, value, 0, value, value)
  
  /**
   * Creates a Stats instance from a set of values.
   */
  def from(values: Double*): Stats = from(values)
}