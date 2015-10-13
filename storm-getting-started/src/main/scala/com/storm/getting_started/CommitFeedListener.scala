package com.storm.getting_started

import java.io.IOException
import java.nio.charset.Charset
import java.util

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{Fields, Values}
import org.apache.commons.io.IOUtils
import scala.collection.JavaConverters._

class CommitFeedListener extends BaseRichSpout {

  var outputCollector: SpoutOutputCollector = _
  var commits: List[String] = _

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("commit"))
  }

  override def open(configMap: util.Map[_, _],
                    context: TopologyContext,
                    outputCollector: SpoutOutputCollector): Unit = {
    this.outputCollector = outputCollector
    try {
      commits = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("changelog.txt"),
        Charset.defaultCharset().name()).asScala.toList
    }
    catch {
      case e: IOException => throw new RuntimeException(e)
    }
  }

  override def nextTuple(): Unit = {
    commits.foreach(commit => outputCollector.emit(new Values(commit)))
  }

}
