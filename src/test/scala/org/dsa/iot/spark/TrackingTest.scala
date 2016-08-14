package org.dsa.iot.spark

import scala.util.Random
import java.io.PrintWriter
import scala.io.Source

object TrackUtils {

  /**
   * Randomly chooses one of the items in the collection. The odds of a particular item to be chosen are proportional
   * to its "weight" (the second element in each tuple).
   */
  def choose[T](coll: Iterable[(T, Int)]): (T, Int) = {
    val pairs = coll.toArray
    val totals = pairs.map(_._2).scanLeft(0)(_ + _).tail
    val found = java.util.Arrays.binarySearch(totals, Random.nextInt(totals.last) + 1)
    val index = if (found >= 0) found else -(found + 1)
    pairs(index)
  }
}

/**
 * A point in a grid.
 */
case class Point(x: Int, y: Int) {
  /**
   * Calculates the distance to another point.
   */
  def distanceTo(p: Point) = Move(this, p).length
}

/**
 * A relative move from one point to another.
 */
case class Move(deltaX: Int, deltaY: Int) {

  /**
   * The length of the move vector.
   */
  lazy val length = math.sqrt(deltaX * deltaX + deltaY * deltaY)

  /**
   * The theta (angle) of the move.
   */
  lazy val theta = math.atan2(deltaY, deltaX)

  /**
   * Scales the move to a given ratio.
   */
  def scale(ratio: Double) = {
    val dx = math.round(deltaX * ratio).toInt
    val dy = math.round(deltaY * ratio).toInt
    Move(dx, dy)
  }

  /**
   * Calculates the next point after the move.
   */
  def apply(point: Point) = Point(point.x + deltaX, point.y + deltaY)
}

/**
 * Move companion object.
 */
object Move {
  /**
   * Creates a relative move between the two points.
   */
  def apply(from: Point, to: Point): Move = apply(to.x - from.x, to.y - from.y)
}

/**
 * Stores move history and predicts a possible next move.
 */
class MoveSet {
  private val history = collection.mutable.Map.empty[Move, Int]

  /**
   * Records a move to the location history.
   */
  def addMove(m: Move): MoveSet = {
    val count = history.getOrElse(m, 0) + 1
    history(m) = count
    this
  }

  /**
   * Records a move to the location history.
   */
  def addMove(deltaX: Int, deltaY: Int): MoveSet = addMove(Move(deltaX, deltaY))

  /**
   * The number of moves stored.
   */
  def count = history.values.sum

  /**
   * Checks if there's any recorded move history.
   */
  def canPredict = !history.isEmpty

  /**
   * Predicts the next move.
   */
  def predict = {
    assert(canPredict, "No recorded move history")
    TrackUtils.choose(history)._1
  }
}

/**
 * Stores move history for various approach angles for a single location.
 */
class LocationMemory {
  private val history = collection.mutable.Map.empty[Option[Double], MoveSet]

  /**
   * Adds a move to the location memory, given the approach angle from which it came to this location.
   */
  def addMove(move: Move, theta: Option[Double]): LocationMemory = {
    val memory = history.getOrElseUpdate(theta, new MoveSet)
    memory.addMove(move)
    this
  }

  /**
   * Adds a move to the location memory with an optional approach angle from which it came to this location.
   */
  def addMove(deltaX: Int, deltaY: Int, theta: Option[Double] = None): LocationMemory =
    addMove(Move(deltaX, deltaY), theta)

  /**
   * Checks if there's any recorded move history.
   */
  def canPredict = !history.isEmpty && history.values.map(_.count).sum > 0

  /**
   * Selects the appropriate MoveMemory for a given approach angle.
   */
  def selectMoveSet(theta: Option[Double]) = {
    assert(canPredict, "No recorded move history")

    history.get(theta) getOrElse {
      theta match {
        case None => TrackUtils.choose(history.values.map(mm => (mm, mm.count)))._1
        case Some(angle) =>
          val angles = history.keys.collect { case Some(a) => a }
          val key = findClosestAngle(angle, angles)
          history(key)
      }
    }
  }

  /**
   * Predicts the next move.
   */
  def predict(theta: Option[Double]) = selectMoveSet(theta).predict

  /**
   * Finds the angle in the list closest to this one.
   */
  private def findClosestAngle(angle: Double, angles: Iterable[Double]) = {
    val deltas = angles map { theta =>
      val delta = angle - theta
      val atan = math.atan2(math.sin(delta), math.cos(delta))
      (theta, math.abs(atan))
    }
    deltas.toList.sortBy(_._2).map(_._1).headOption
  }
}

/**
 * Movement tracking engine.
 */
class Tracker {
  private var location: Option[Point] = None
  private var lastMove: Option[Move] = None

  private val history = collection.mutable.Map.empty[Point, LocationMemory]

  /**
   * The current position of the tracker.
   */
  def currentPosition = location

  /**
   * Clears the current location and all move history.
   */
  def clear: Tracker = {
    history.clear
    location = None
    lastMove = None
    this
  }

  /**
   * Resets the current location, starting a new sequence.
   */
  def reset(p: Point): Tracker = {
    location = Some(p)
    lastMove = None
    this
  }

  /**
   * Resets the current location, starting a new sequence.
   */
  def reset(x: Int, y: Int): Tracker = reset(Point(x, y))

  /**
   * Moves to the next location. The current location should have already been set.
   */
  def moveTo(p: Point, record: Boolean): Tracker = {
    assert(location.isDefined, "Tracking sequence not initialized, use reset()")

    val loc = location.get

    val move = Move(loc, p)

    if (record) {
      val memory = history.getOrElseUpdate(loc, new LocationMemory)
      memory.addMove(move, lastTheta)
    }

    lastMove = Some(move)
    location = Some(p)
    this
  }

  /**
   * Moves to the next location. The current location should have already been set.
   */
  def moveTo(x: Int, y: Int, record: Boolean): Tracker = moveTo(Point(x, y), record)

  /**
   * Predicts the next move.
   */
  def predict = {
    assert(location.isDefined, "Tracking sequence not initialized, use reset()")
    val loc = location.get

    val move = if (history.isEmpty)
      randomMove
    else if (history.contains(loc)) {
      val memory = history(loc)
      memory.predict(lastTheta)
    } else {
      val closest = findClosestLocation(loc)
      val directMove = Move(loc, closest)
      directMove.scale(1.0 / directMove.length)
    }

    move(loc)
  }

  private def randomMove = {
    val deltaX = Random.nextInt(3) - 1
    val deltaY = Random.nextInt(3) - 1
    Move(deltaX, deltaY)
  }

  private def findClosestLocation(p: Point): Point = {
    val deltas = history.keys.toList map { k => (k, k.distanceTo(p)) }
    deltas.sortBy(_._2).map(_._1).head
  }

  private def lastTheta = lastMove map (_.theta)

  override def toString = s"""Tracking(loc=${location.getOrElse("?")}, last=${lastMove.getOrElse("?")})"""
}

/**
 * App entry point.
 */
object TrackingTest extends App {

  if (args.length < 4) {
    println("Usage: TrackingTest input-file initX initY stepCount")
    sys.exit(0)
  }

  val track = new Tracker

  // read input file
  val src = Source.fromFile(args(0))
  track.reset(readLine(src.getLines.next))
  src.getLines.drop(1) foreach { line =>
    track.moveTo(readLine(line), true)
  }

  // set initial location
  track.reset(args(1).toInt, args(2).toInt)

  // generate predictions
  printCurrentPos(track)
  (1 to args(3).toInt) foreach { index =>
    val next = track.predict
    printCurrentPos(track)
    track.moveTo(next, false)
  }

  private def readLine(line: String) = {
    val parts = line.split(",").map(_.trim)
    Point(parts(0).toInt, parts(1).toInt)
  }
  
  private def printCurrentPos(track: Tracker) = track.currentPosition foreach { p => println(s"${p.x},${p.y}") }
}

//object TrackingTest extends App {
//
//  def track1() = {
//    val out = new PrintWriter("/Users/vladorz/Downloads/stash/track1.txt")
//    (0 to 100) foreach (n => out.println(s"$n,$n"))
//    (100 to 0 by -1) foreach (n => out.println(s"$n,100"))
//    (0 to 100) foreach (n => out.println(s"$n,${100 - n}"))
//    (100 to 0 by -1) foreach (n => out.println(s"$n,0"))
//    out.close
//  }
//
//  def track2() = {
//    val out = new PrintWriter("/Users/vladorz/Downloads/stash/track2.txt")
//    (75 to 100) foreach (n => out.println(s"$n,$n"))
//    (100 to 0 by -1) foreach (n => out.println(s"$n,100"))
//    (0 to 100) foreach (n => out.println(s"$n,${100 - n}"))
//    (100 to 0 by -1) foreach (n => out.println(s"$n,0"))
//    out.close
//  }
//
//  def track3() = {
//    val out = new PrintWriter("/Users/vladorz/Downloads/stash/track3.txt")
//    (0 to 10) foreach (n => out.println(s"${85 - n},${65 + n}"))
//    (75 to 100) foreach (n => out.println(s"$n,$n"))
//    (100 to 0 by -1) foreach (n => out.println(s"$n,100"))
//    (0 to 100) foreach (n => out.println(s"$n,${100 - n}"))
//    (100 to 0 by -1) foreach (n => out.println(s"$n,0"))
//    out.close
//  }
//
//  def track4() = {
//    val out = new PrintWriter("/Users/vladorz/Downloads/stash/track4.txt")
//    (25 to 50) foreach (n => out.println(s"$n,${n - 20}"))
//    (30 to 50) foreach (n => out.println(s"50,$n"))
//    (50 to 80) foreach (n => out.println(s"$n,50"))
//    out.close
//  }
//
//  def track5() = {
//    val out = new PrintWriter("/Users/vladorz/Downloads/stash/track5.txt")
//    (0 to 50) foreach (n => out.println(s"50,$n"))
//    (50 to 10 by -1) foreach (n => out.println(s"$n,50"))
//    out.close
//  }
//
//  def track6() = {
//    val out = new PrintWriter("/Users/vladorz/Downloads/stash/track6.txt")
//    (10 to 50) foreach (n => out.println(s"50,$n"))
//    (50 to 75) foreach (n => out.println(s"$n,50"))
//    out.close
//  }
//
//}