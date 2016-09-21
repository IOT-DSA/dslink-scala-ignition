package org.dsa.iot.rx.script

import org.mvel2.ParserContext
import com.ignition.script.ScriptFunctions
import org.mvel2.integration.impl.MapVariableResolverFactory
import collection.JavaConverters._
import scala.reflect.runtime.universe._
import java.io.File
import scala.tools.nsc.interpreter.IMain

/**
 * Available scripting dialects.
 */
object ScriptDialect extends Enumeration {

  abstract class ScriptDialect(val name: String) extends super.Val(name) {
    def execute[T: TypeTag](code: String, context: Map[String, Any]): T
  }
  implicit def valueToDialect(v: Value) = v.asInstanceOf[ScriptDialect]

  /**
   * Mvel.
   */
  val MVEL = new ScriptDialect("MVEL") {
    def execute[T: TypeTag](code: String, context: Map[String, Any]): T = {
      val varMap = new java.util.HashMap[String, Any]
      varMap.putAll(context.asJava)
      val factory = new MapVariableResolverFactory(varMap)
      val tag = implicitly[TypeTag[T]]
      val targetType = try {
        tag.mirror.runtimeClass(tag.tpe).asInstanceOf[Class[T]]
      } catch {
        case e : java.lang.ClassNotFoundException => classOf[AnyRef].asInstanceOf[Class[T]]
      }
      org.mvel2.MVEL.eval(code, ScriptFunctions, factory, targetType)
    }
  }

  /**
   * Java.
   */
  val JAVA = new ScriptDialect("Java") {
    def execute[T: TypeTag](code: String, context: Map[String, Any]): T = ???
  }

  /**
   * Groovy.
   */
  val GROOVY = new ScriptDialect("Groovy") {
    def execute[T: TypeTag](code: String, context: Map[String, Any]): T = ???
  }

  /**
   * Scala.
   */
  val SCALA = new ScriptDialect("Scala") {
    val main = new IMain(ScalaDialect.settings)

    def execute[T: TypeTag](code: String, context: Map[String, Any]): T = {
      main.beSilentDuring {
        context foreach {
          case (name, value) => main.bind(name, value)
        }
        val lines = code.split("\\r?\\n")
        lines foreach main.interpret
      }
      main.valueOfTerm(main.mostRecentVar).get.asInstanceOf[T]
    }
  }

  /**
   * XPath.
   */
  val XPATH = new ScriptDialect("XPath") {
    def execute[T: TypeTag](code: String, context: Map[String, Any]): T = ???
  }

  /**
   * Json Path.
   */
  val JPATH = new ScriptDialect("JsonPath") {
    def execute[T: TypeTag](code: String, context: Map[String, Any]): T = ???
  }
}

object MvelDialect {
  lazy val parserContext = {
    val pctx = new ParserContext
    ScriptFunctions.getClass.getDeclaredMethods foreach { method =>
      pctx.addImport(method.getName, method)
    }
    pctx
  }
}

object ScalaDialect {
  lazy val settings = {
    val s = new scala.tools.nsc.Settings
    s.usejavacp.value = false
    s.deprecation.value = true
    s.classpath.value += File.pathSeparator + System.getProperty("java.class.path")
    s
  }
}