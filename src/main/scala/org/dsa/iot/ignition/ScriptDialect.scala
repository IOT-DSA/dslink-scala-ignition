package org.dsa.iot.ignition

import org.mvel2.ParserContext
import com.ignition.script.ScriptFunctions
import org.mvel2.integration.impl.MapVariableResolverFactory
import collection.JavaConverters._
import scala.reflect.runtime.universe._

/**
 * Available Script dialects.
 */
object ScriptDialect extends Enumeration {

  abstract class ScriptDialect(name: String) extends super.Val(name) {
    def execute[T: TypeTag](code: String, context: Map[String, Any]): T
  }
  implicit def valueToDialect(v: Value) = v.asInstanceOf[ScriptDialect]

  /**
   * Mvel.
   */
  val MVEL = new ScriptDialect("MVEL") {
    def execute[T: TypeTag](code: String, context: Map[String, Any]): T = {
      val factory = new MapVariableResolverFactory(context.asJava)
      val tag = implicitly[TypeTag[T]]
      org.mvel2.MVEL.eval(code, ScriptFunctions, factory, tag.mirror.runtimeClass(tag.tpe).asInstanceOf[Class[T]])
    }
  }

  /**
   * Java.
   */
  val JAVA = new ScriptDialect("Java") {
    def execute[T: TypeTag](code: String, context: Map[String, Any]): T = ???
  }

  /**
   * Scala.
   */
  val SCALA = new ScriptDialect("Scala") {
    def execute[T: TypeTag](code: String, context: Map[String, Any]): T = ???
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