package stormbook

import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Values, Fields, Tuple}
import com.google.code.geocoder.model.LatLng

class TimeIntervalExtractor extends BaseBasicBolt {

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("time-interval", "geocode"))
  }

  override def execute(tuple: Tuple, outputCollector: BasicOutputCollector): Unit = {
    val time = tuple.getLongByField("time")
    val geocode = tuple.getValueByField("geocode").asInstanceOf[LatLng]
    val timeInterval = time / (15 * 1000)
    outputCollector.emit(new Values(timeInterval, geocode))
  }

}
