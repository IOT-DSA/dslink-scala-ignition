package org.dsa.iot.ignition

import scala.tools.nsc.interpreter._
import scala.tools.nsc._
import scala.util.Try
import java.io.File
import scala.annotation.tailrec
import java.io.EOFException

object Repl {
  val prompt = ""
  
  val settings = buildSettings
  val main = new IMain(settings)
  
  def run() = {
    main.interpret("import org.dsa.iot.ignition._")
    main.interpret("import org.dsa.iot.ignition.core._")
    main.interpret("import Main.requester")
    main.interpret("import Main.responder")
    
    loop(str => main.interpret(str))
  }
  
  def loop(action: (String) => Unit) {
    @tailrec def inner() {
      Console.print(prompt)
      val line = try scala.io.StdIn.readLine catch { case _: EOFException => null }
      if (line != null && line != "") {
        action(line)
        inner()
      }
    }
    inner()
  }  
  
  private def buildSettings = {
    val s = new Settings
    s.usejavacp.value = false
    s.deprecation.value = true
    s.classpath.value += File.pathSeparator + System.getProperty( "java.class.path" )
    s
  }  
}