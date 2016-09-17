package org.dsa.iot.ignition.spark

import com.ignition.frame.SparkRuntime
import com.ignition.frame.HttpMethod
import com.ignition.frame.HttpMethod.HttpMethod

/**
 * HTTP REST Client, executes one request per row.
 */
class RestClient(implicit rt: SparkRuntime) extends RxFrameTransformer {
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
   * Creates a new RestClient instance with the specified HTTP method, body, headers, and
   * result/status/headers return fields.
   */
  def apply(method: HttpMethod.HttpMethod = HttpMethod.GET, body: Option[String] = None,
            headers: Map[String, String] = Map.empty,
            resultField: Option[String] = Some("result"),
            statusField: Option[String] = Some("status"),
            headersField: Option[String] = None)(implicit rt: SparkRuntime): RestClient = {
    val block = new RestClient
    block.method <~ method
    block.body <~ body
    block.headers <~ (headers.toSeq: _*)
    block.resultField <~ resultField
    block.statusField <~ statusField
    block.headersField <~ headersField
    block
  }
}