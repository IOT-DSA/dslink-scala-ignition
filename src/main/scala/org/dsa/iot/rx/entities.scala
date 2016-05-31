package org.dsa.iot.rx

import org.slf4j.LoggerFactory

import rx.lang.scala.{ Observable, Subscription }
import rx.lang.scala.subjects.BehaviorSubject

/**
 * Provides logging support.
 */
trait Logging {
  private val log = LoggerFactory.getLogger(getClass)

  def trace(message: => String, args: Any*) = log.trace(message, a2o(args): _*)
  def debug(message: => String, args: Any*) = log.debug(message, a2o(args): _*)
  def info(message: => String, args: Any*) = log.info(message, a2o(args): _*)
  def warn(message: => String, args: Any*) = log.warn(message, a2o(args): _*)
  def warn(message: => String, err: Throwable) = log.warn(message, err)
  def error(message: => String, args: Any*) = log.error(message, a2o(args): _*)
  def error(message: => String, err: Throwable) = log.error(message, err)

  private def a2o(x: Seq[Any]): Seq[Object] = x.map(_.asInstanceOf[java.lang.Object])
}

/**
 * A block that emits items of type R.
 */
trait RxBlock[R] {
  /**
   * Returns the block's output as an Observable.
   */
  def output: Observable[R]

  /**
   * Restarts the block.
   */
  def reset(): Unit

  /**
   * Stops the data flow through the block.
   */
  def shutdown(): Unit
}

/**
 * Top of RxBlock hierarchy.
 * 
 * @param A the type of the attribute set (a single class or a tuple).
 * @param T the type of the inputs a single Observable or a tuple of Observables.
 * @param R the type of the result.
 */
abstract class AbstractRxBlock[A, T, R] extends RxBlock[R] with Logging {

  /* abstracts */

  /**
   * Combines the attribute streams into a single observable.
   */
  protected def combineAttributes: Observable[A]

  /**
   * Returns the input streams.
   */
  protected def combineInputs: T

  /**
   * The evaluating function of a block as "attributes => inputs => output."
   */
  protected def evaluator(attrs: A): T => Observable[R]

  /* implementation */

  private val subj = BehaviorSubject[R]()
  private var outSubscription: Option[Subscription] = None
  private var attrSubscription: Option[Subscription] = None

  lazy val output = withEvents(subj).share

  def reset() = {
    unsubsribeInputs
    unsubscribeAttributes

    attrSubscription = Some(combineAttributes subscribe { attrs =>
      unsubsribeInputs
      outSubscription = Some(evaluator(attrs)(combineInputs) subscribe (subj.onNext(_)))
    })
  }

  def shutdown() = {
    unsubsribeInputs
    unsubscribeAttributes
    subj.onCompleted
  }

  protected def unsubscribeAttributes() = attrSubscription foreach (_.unsubscribe)
  protected def unsubsribeInputs() = outSubscription foreach (_.unsubscribe)

  protected def withEvents(stream: Observable[R]): Observable[R] = {
    val id = getClass.getSimpleName + "." + stream.getClass.getSimpleName + "." + (math.abs(stream.hashCode) % 1000)
    withEvents(id)(stream)
  }

  protected def withEvents(id: String)(stream: Observable[R]): Observable[R] = {
    def render(state: String) = debug(s"[$id] $state")

    stream
      .doOnCompleted(render("completed"))
      .doOnSubscribe(render("subscribed to"))
      .doOnTerminate(render("terminated"))
      .doOnUnsubscribe(render("unsubscribed from"))
  }

  /**
   * Connector for attributes and inputs.
   */
  case class Port[X](name: String) extends Logging {
    var in: Observable[X] = Observable.never

    def set(value: X) = synchronized {
      in = Observable.just(value)
      info(s"$name.value set to $value")
      reset
    }

    def bind(block: RxBlock[X]) = synchronized {
      in = block.output
      info(s"$name bound to $block")
      reset
    }

    def unbind() = synchronized {
      in = Observable.never
      info(s"$name unbound")
      reset
    }
  }

  object Port {
    def apply[X]: Port[X] = apply("?")
  }

  /**
   * Connector for a list of attributes or inputs.
   */
  case class PortList[X](name: String) extends Logging {
    var ports: List[Port[X]] = Nil
    
    def set(values: Iterable[X]) = synchronized {
      ports = List.fill(values.size)(Port[X])
      ports zip values foreach {
        case (port, value) => port.set(value)
      }
      info(s"$name set to $values")
      reset
    }

    def bind(blocks: Iterable[RxBlock[X]]) = synchronized {
      ports = List.fill(blocks.size)(Port[X])
      ports zip blocks foreach {
        case (port, src) => port.bind(src)
      }
      info(s"$name bound to $blocks")
      reset
    }

    def unbind() = synchronized {
      ports foreach (_.unbind)
      info(s"$name unbound")
      reset
    }
  }
  
  object PortList {
    def apply[X]: PortList[X] = apply("?")
  }
}