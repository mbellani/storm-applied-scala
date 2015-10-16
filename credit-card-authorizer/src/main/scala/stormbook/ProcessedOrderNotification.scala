package stormbook

import java.util

import backtype.storm.task.TopologyContext
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Tuple
import org.slf4j.{LoggerFactory, Logger}


class ProcessedOrderNotification extends BaseBasicBolt {

  private var notifier: Notifier = _


  override def prepare(stormConf: util.Map[_, _], context: TopologyContext): Unit = {
    notifier = new Notifier
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
  }

  override def execute(tuple: Tuple, outputCollector: BasicOutputCollector): Unit = {
    val order = tuple.getValueByField("order").asInstanceOf[Order]
    notifier.processed(order)
  }
}

class Notifier {
  val logger: Logger = LoggerFactory.getLogger(classOf[Notifier])

  def processed(order: Order): Unit = {
    logger.info("order processed {}", order)
  }
}
