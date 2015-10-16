package stormbook

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Tuple, Values}
import org.slf4j.{Logger, LoggerFactory}

class AuthorizeCreditCard extends BaseRichBolt {

  private var authService: AuthService = _
  private var orders: Orders = _
  private var outputCollector: OutputCollector = _
  private val logger: Logger = LoggerFactory.getLogger(classOf[AuthorizeCreditCard])


  override def prepare(conf: util.Map[_, _],
                       context: TopologyContext,
                       outputCollector: OutputCollector): Unit = {
    this.authService = new AuthService
    this.orders = new Orders
    this.outputCollector = outputCollector
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("order"))
  }

  override def execute(tuple: Tuple): Unit = {
    try {
      val order = tuple.getValueByField("order").asInstanceOf[Order]
      val authResponse = authService.authorize(order)

      new AuthResponseHandler((success) => orders.readyToShip(success),
        (failure) => orders.denied(failure)).handle(authResponse)
      outputCollector.emit(tuple, new Values(order))
      outputCollector.ack(tuple)
    }
    catch {
      case e => outputCollector.fail(tuple)
    }
  }
}


class Orders {
  private val logger: Logger = LoggerFactory.getLogger(classOf[Orders])

  def readyToShip(order: Order): Unit = {
    logger.info("order is ready to ship {}", order)
  }

  def denied(order: Order): Unit = {
    logger.info("order is denied {}", order)
  }
}
