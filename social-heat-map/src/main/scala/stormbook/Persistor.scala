package stormbook

import java.lang.Long
import java.util

import backtype.storm.task.TopologyContext
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.tuple.Tuple
import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.commands.kv.StoreValue
import com.basho.riak.client.core.query.{RiakObject, Location, Namespace}
import com.basho.riak.client.core.util.BinaryValue
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.code.geocoder.model.LatLng
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

import scala.collection.mutable.ArrayBuffer

class Persistor extends BaseBasicBolt {
  private var riak: RiakClient = _
  private var objectMapper: ObjectMapper = _
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def prepare(config: util.Map[_, _], context: TopologyContext): Unit = {
    super.prepare(config, context)
    riak = RiakClient.newClient("localhost")
    objectMapper = new ObjectMapper
  }

  override def execute(tuple: Tuple, outputCollector: BasicOutputCollector): Unit = {
    try {
      val timeInterval = tuple.getLongByField("time-interval")
      val hotzones: ArrayBuffer[LatLng] = tuple.getValueByField("hotzones").asInstanceOf[ArrayBuffer[LatLng]]
      logger.info("got hotzones {}", hotzones.size)
      persist(timeInterval, hotzones.map(_.toUrlValue()))
    }
    catch {
      case e: Exception => logger.error("error persisting records {}", e)
    }
  }

  def asRiakObject(hotzones: ArrayBuffer[String]): RiakObject = {
    val json_map = Map("values" -> hotzones.asJava)
    val json = objectMapper.writeValueAsString(json_map.asJava)
    logger.info("persisting {} ", json.substring(0, 100))
    return new RiakObject().setContentType("application/json")
      .setValue(BinaryValue.create(json))
  }


  private def persist(timeInterval: Long, hotzones: ArrayBuffer[String]): Unit = {
    val key = s"checkins-new-$timeInterval"
    logger.info("hotzones {} for key {}", hotzones.length, key)
    val location = new Location(new Namespace("default", "heatmap"), key)
    val value = new StoreValue.Builder(asRiakObject(hotzones)).withLocation(location).build()
    val response  = riak.execute(value)
    logger.info("finished execution ...{}", response.getVectorClock)

  }

  override def cleanup(): Unit = {
    riak.shutdown()
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
  }
}
