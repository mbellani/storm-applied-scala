package com.storm.getting_started

import java.util

import backtype.storm.task.TopologyContext
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.tuple.Tuple
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

class EmailCounter extends BaseBasicBolt {
  private var counts: Map[String, Int] = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext): Unit = {
    super.prepare(stormConf, context)
    counts = new HashMap[String, Int]()
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
  }

  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    val email = tuple.getStringByField("email")
    counts(email) = counts.getOrElse(email, 0) + 1
  }
}
