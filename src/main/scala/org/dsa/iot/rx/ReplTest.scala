package org.dsa.iot.rx

import scala.tools.nsc.interpreter._
import scala.tools.nsc._
import scala.util.Try
import java.io.File

object ReplTest extends App {

  val settings = buildSettings
  val main = new IMain(settings)

  evaluate("""
    | val x = 5
    | val y = 3
    | val z = math.sin(x * y)
    | val m = rx.lang.scala.Observable.just(1)
    """.stripMargin, "m")
    
  def evaluate(code: String, ret: String) = {
    main.beQuietDuring(main.interpret(code))
    val result = main.valueOfTerm(ret)
    println(result)
  }
  
  private def buildSettings = {
    val s = new Settings
    s.usejavacp.value = false
    s.deprecation.value = true
    s.embeddedDefaults[Logging]
    s.classpath.value += File.pathSeparator + System.getProperty( "java.class.path" )
    s
  }
}