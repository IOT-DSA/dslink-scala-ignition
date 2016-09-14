package org.dsa.iot.ignition.core

import scala.collection.JavaConverters.asScalaBufferConverter

import org.dsa.iot.dslink.link.Requester
import org.dsa.iot.dslink.node.actions.table.Row
import org.dsa.iot.rx.AbstractRxBlock
import org.dsa.iot.scala.{ DSAHelper, valueToAny }

import rx.lang.scala.Observable

/**
 * Runs a DQL query and returns the results as a data row (list of values).
 */
class DQLInput(implicit requester: Requester) extends AbstractRxBlock[List[Any]] {
  val query = Port[String]("query")

  protected def compute = query.in flatMap { dql =>
    val obs = DSAHelper.invoke("/downstream/DQL/query", "query" -> dql)

    obs flatMap { evt =>
      val rows = (for {
        rsp <- Option(evt)
        tbl <- Option(rsp.getTable)
        rows <- Option(tbl.getRows) map (_.asScala.toList)
      } yield rows) getOrElse Nil
      Observable.from(rows map rowToList)
    }
  }

  private def rowToList(row: Row) = row.getValues.asScala.toList.map {
    case null => null
    case x    => valueToAny(x)
  }
}

/**
 * Factory for [[DQLInput]] instances.
 */
object DQLInput {

  /**
   * Creates a new DQLInput instance.
   */
  def apply()(implicit requester: Requester): DQLInput = new DQLInput

  /**
   * Creates a new DQLInput instance with the specified query.
   */
  def apply(query: String)(implicit requester: Requester): DQLInput = {
    val block = new DQLInput
    block.query <~ query
    block
  }
}