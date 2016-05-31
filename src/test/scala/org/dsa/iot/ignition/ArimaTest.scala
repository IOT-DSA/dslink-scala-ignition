package org.dsa.iot.ignition

import java.io._
import org.apache.spark.mllib.linalg.Vectors
import com.cloudera.sparkts.models.ARIMA
import scala.io.Source

object ArimaTest extends App {
  
  val elacFile = args(0)

  checkClientData(1)

  def checkClientData(idx: Int) = {
    val lines = Source.fromFile(elacFile).getLines

    val values = lines.map { str =>
      val parts = str.split("\\s*,\\s*")
      parts(idx).toDouble - 26.7734
    }

    val refData = Vectors.dense(values.take(1220).toArray)
    printToFile(refData, s"${elacFile}_${idx}_ref.txt")
    
    val model = ARIMA.autoFit(refData)

    println(s"d=${model.d}, p=${model.p}, q=${model.q}")
    println(s"coeffs=${model.coefficients.toList}")
    println(s"intercept=${model.hasIntercept}, stationary=${model.isStationary}")
    
    val predicted = model.forecast(refData, 1000)
    printToFile(predicted, s"${elacFile}_${idx}_model.txt")
  }

  def checkRefData() = {

    // generate reference data
    val refSize = 100
    val refValues = (0 until refSize) map { x =>
      10 * math.sin(x) + math.cos(x * 10.0) + 0.5 * math.sin(x * 15.0)
    }
    val refData = Vectors.dense(refValues.toArray)

    // generate ARIMA model
    val model = ARIMA.autoFit(refData)

    println(s"d=${model.d}, p=${model.p}, q=${model.q}")
    println(s"coeffs=${model.coefficients.toList}")
    println(s"intercept=${model.hasIntercept}, stationary=${model.isStationary}")

    val predicted = model.forecast(refData, 100)

    printToFile(refData, s"${elacFile}_ref.txt")
    printToFile(predicted, s"${elacFile}_predicted.txt")
  }

  private def printToFile(data: org.apache.spark.mllib.linalg.Vector, filename: String) = {
    val pw = new PrintWriter(new File(filename))
    data.toDense.toArray.toList.zipWithIndex.foreach {
      case (y, x) => pw.println(s"$x, $y")
    }
    pw.close
  }
}