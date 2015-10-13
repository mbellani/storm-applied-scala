package stormbook

import java.{lang, util}

import backtype.storm.{Constants, Config}
import backtype.storm.task.TopologyContext
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Values, Fields, Tuple}
import com.google.code.geocoder.model.LatLng
import org.slf4j.{LoggerFactory, Logger}


import scala.collection.mutable

class HeatmapBuilder extends BaseBasicBolt {
  var heatmap: mutable.Map[Long, mutable.ArrayBuffer[LatLng]] = _
  val logger: Logger = LoggerFactory.getLogger(classOf[HeatmapBuilder])

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext): Unit = {
    super.prepare(stormConf, context)
    heatmap = new mutable.HashMap[Long, mutable.ArrayBuffer[LatLng]]()

  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("time-interval", "hotzones"))
  }

  override def getComponentConfiguration: util.Map[String, Object] = {
    val conf = new Config
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, new Integer(10))
    conf
  }

  private def emitHeatMap(outputCollector: BasicOutputCollector): Unit = {
    val emitUptoTimeInterval = System.currentTimeMillis() / (1000 * 15)
    heatmap.foreach(entry => {
      val interval: Long = entry._1
      val hotzones: Seq[LatLng] = entry._2

      if (interval <= emitUptoTimeInterval) {
        outputCollector.emit(new Values(interval.asInstanceOf[java.lang.Long], hotzones))
        heatmap -= interval
      }
    })
  }

  override def execute(tuple: Tuple, outputCollector: BasicOutputCollector): Unit = {
    if (isTickTuple(tuple)) {
      emitHeatMap(outputCollector)
      return
    }
    val timeIntereval = tuple.getLongByField("time-interval")
    val geocode = tuple.getValueByField("geocode").asInstanceOf[LatLng]
    heatmap.getOrElseUpdate(timeIntereval, mutable.ArrayBuffer[LatLng]()) += geocode

  }


  private def isTickTuple(tuple: Tuple): Boolean = {
    return tuple.getSourceComponent.equals(Constants.SYSTEM_COMPONENT_ID) &&
      tuple.getSourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID)
  }
}
