package stormbook

import java.util

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{Fields, Values}

import scala.io.Source.fromInputStream

class Checkins extends BaseRichSpout {

  private var checkins: List[String] = _
  private var outputCollector: SpoutOutputCollector = _
  private var nextEmitIndex: Int = _

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("time", "address"))
  }

  override def open(configMap: util.Map[_, _],
                    context: TopologyContext,
                    outputCollector: SpoutOutputCollector): Unit = {
    this.outputCollector = outputCollector
    this.nextEmitIndex = 0

    val checkinStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("checkins.txt")
    checkins = fromInputStream(checkinStream)
      .getLines()
      .toList
  }

  override def nextTuple(): Unit = {
    val parts = checkins(nextEmitIndex).split(",")
    val time = parts(0).toLong
    val address = parts(1)
    outputCollector.emit(new Values(time.asInstanceOf[java.lang.Long], address))
    nextEmitIndex = (nextEmitIndex + 1) % checkins.size
  }


}
