package com.storm.getting_started

import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Values, Fields, Tuple}

class EmailExtractor extends BaseBasicBolt {

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("email"))
  }

  override def execute(tuple: Tuple, outputCollector: BasicOutputCollector): Unit = {
    val commit = tuple.getStringByField("commit")
    val email = commit.split(" ")(0)
    outputCollector.emit(new Values(email))
  }
}
