package org.dsa.iot.spark

import scala.util.control.NonFatal
import org.apache.spark._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext

object LoadTest extends App {

  val conf = new SparkConf().setAppName("load-test").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val path = System.getProperty("user.home") + "/Downloads/BuildingLightsLoad.csv"
  val lines = (sc textFile path zipWithIndex) filter (_._2 > 0)
  
  val fmt = DateTimeFormat.forPattern("dd-MMM-yy h:mm:ss a z")
  
  val partitioner = new HashPartitioner(sc.defaultParallelism)
  
  // load the file and parse each line
  val src = lines map {
    case (line, rowIdx) => try {
      val parts = line.split(",")
      val millis = DateTime.parse(parts(0), fmt).withSecondOfMinute(0).getMillis
      val flags = parts(1).toInt
      val status = parts(2).toInt
      val value = parts(3).toDouble
      (millis, flags, status, value)
    } catch {
      case NonFatal(e) => println(s"Error in line #rowIdx: $e"); null
    }
  }
  
  // remove records with 0 values and sort by time
  val cleaned = src.filter(_._4 != 0).map(t => (t._1, t._4)).sortBy(_._1)
  val prev = cleaned.zipWithIndex.map(t => (t._2, t._1))
  val next = cleaned.zipWithIndex.map(t => (t._2 - 1, t._1))
  
  val joined = prev join next map {
    case (idx, ((time, prev), (_, next))) => (time, prev, next)
  }
  joined.toDF("time", "prev", "next").show

//.partitionBy(partitioner).persist  
  // find interpolation coefficients
  
//  // find coefficients
//  val sorted = cleaned.sortBy(_._1, true)
//  
  // create an RDD of reference time points
  val minMillis = cleaned.map(_._1).min
  val maxMillis = cleaned.map(_._1).max
  println(minMillis)
  println(maxMillis)
//  val refTimes = sc.parallelize(Seq.range(minMillis, maxMillis, 15 * 60 * 1000)).map(t => (t, true))
//  
//  // join the time RDD and data RDD
//  val joined = refTimes leftOuterJoin cleaned map {
//    case (millis, (_, value)) => (millis, value)
//  }
  
//  val cleanedDF = cleaned.toDF("time", "value").sort("time")
  
//  joined filter (_._2.isEmpty) take 10 foreach println

  sc.stop
}