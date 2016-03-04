package org.dsa.iot.ignition.step

import scala.xml.{ Elem, Node }
import org.apache.spark.sql.DataFrame
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2Assoc, seq2jvalue, string2jvalue }
import org.json4s.jvalue2monadic
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq
import org.dsa.iot.ignition.Main._
import com.ignition.frame._
import org.dsa.iot.DSAHelper

/**
 * Posts updates to DSA node tree.
 */
case class DSAOutput(fields: Iterable[(String, String)]) extends FrameTransformer {
  import DSAOutput._

  def add(name: String, path: String) = copy(fields = this.fields.toSeq :+ (name -> path))
  def %(name: String, path: String) = add(name, path)

  def add(tuple: (String, String)) = copy(fields = this.fields.toSeq :+ tuple)
  def %(tuple: (String, String)) = add(tuple)

  protected def compute(arg: DataFrame)(implicit runtime: SparkRuntime): DataFrame = {
    val df = if (runtime.previewMode) arg.limit(FrameStep.previewSize) else arg
    df.collect foreach { row =>
      fields foreach {
        case (name, path) => DSAHelper updateNode path -> row.getAs[Any](name)
      }
    }
    df
  }

  def toXml: Elem =
    <node>
      <fields>
        { fields map (f => <field name={ f._1 }>{ f._2 }</field>) }
      </fields>
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("fields" -> fields.map { f =>
    ("name" -> f._1) ~ ("path" -> f._2)
  })
}

/**
 * DSA Output companion object.
 */
object DSAOutput {
  val tag = "dsa-output"

  def apply(fields: (String, String)*): DSAOutput = new DSAOutput(fields)

  def fromXml(xml: Node) = {
    val fields = (xml \ "fields" \ "field") map { node =>
      val name = node \ "@name" asString
      val path = node asString

      name -> path
    }
    apply(fields)
  }

  def fromJson(json: JValue) = {
    val fields = (json \ "fields" asArray) map { node =>
      val name = node \ "name" asString
      val path = node \ "path" asString

      name -> path
    }
    apply(fields)
  }
}