package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import com.ignition.frame.HttpMethod
import com.ignition.frame.HttpMethod.HttpMethod

import org.dsa.iot.scala.Having

/**
 * HTTP REST Client, executes one request per row.
 */
class RestClient(implicit rt: SparkRuntime) extends RxFrameTransformer {

  def url(str: String): RestClient = this having (url <~ str)
  def method(md: HttpMethod): RestClient = this having (method <~ md)
  def headers(tuples: (String, String)*): RestClient = this having (headers <~ tuples)

  def body(str: String): RestClient = this having (body <~ Some(str))
  def resultField(str: String): RestClient = this having (resultField <~ Some(str))
  def noResultField(): RestClient = this having (resultField <~ None)
  def statusField(str: String): RestClient = this having (statusField <~ Some(str))
  def noStatusField(): RestClient = this having (statusField <~ None)
  def headersField(str: String): RestClient = this having (headersField <~ Some(str))
  def noHeadersField(): RestClient = this having (headersField <~ None)

  val url = Port[String]("url")
  val method = Port[HttpMethod]("method")
  val body = Port[Option[String]]("body")
  val headers = PortList[(String, String)]("headers")
  val resultField = Port[Option[String]]("resultField")
  val statusField = Port[Option[String]]("statusField")
  val headersField = Port[Option[String]]("headersField")

  protected def compute = (url.in combineLatest method.in combineLatest body.in
    combineLatest headers.combinedIns combineLatest resultField.in combineLatest statusField.in
    combineLatest headersField.in) flatMap {
      case ((((((url, method), body), headers), resultField), statusField), headersField) =>
        val rc = com.ignition.frame.RestClient(url, method, body, headers.toMap, resultField,
          statusField, headersField)
        doTransform(rc)
    }
}

/**
 * Factory for [[RestClient]] instances.
 */
object RestClient {

  /**
   * Creates a new RestClient instance with HTTP GET method, no body and headers.
   */
  def apply()(implicit rt: SparkRuntime): RestClient = {
    val block = new RestClient
    block.method <~ HttpMethod.GET
    block.body <~ None
    block.headers <~ Map.empty
    block.resultField <~ Some("result")
    block.statusField <~ Some("status")
    block.headersField <~ None
    block
  }
}