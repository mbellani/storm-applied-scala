package stormbook

import java.util

import backtype.storm.task.TopologyContext
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Values, Fields, Tuple}
import com.google.code.geocoder.model.{GeocoderStatus, LatLng}
import com.google.code.geocoder.{GeocoderRequestBuilder, Geocoder}

class GeocodeLookup extends BaseBasicBolt {
  var geocoder: Geocoder = _
  //local cache, receive over query limit from google fast, topology makes too many requests.
  val geocodes: Map[String, LatLng] = Map("287 Hudson St New York NY 10013" -> new LatLng("40.725612", "-74.007916"),
    "155 Varick St New York NY 10013" -> new LatLng("40.726276", "-74.006000"),
    "222 W Houston St New York NY 10013" -> new LatLng("40.728786", "-74.004732"),
    "5 Spring St New York NY 10013" -> new LatLng("40.721300", "-73.994316"),
    "148 West 4th St New York NY 10013" -> new LatLng("40.731355", "-74.000650")
  )

  override def prepare(stormConf: util.Map[_, _],
                       context: TopologyContext): Unit = {
    super.prepare(stormConf, context)
    geocoder = new Geocoder()
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("time", "geocode"))
  }

  override def execute(tuple: Tuple,
                       outputCollector: BasicOutputCollector): Unit = {
    val address = tuple.getStringByField("address")
    val time = tuple.getLongByField("time")
    outputCollector.emit(new Values(time, geocodes(address)))
  }

  private def geocode(address: String): LatLng = {
    val request = new GeocoderRequestBuilder()
      .setAddress(address)
      .setLanguage("en")
      .getGeocoderRequest

    val response = geocoder.geocode(request)

    if (GeocoderStatus.OK.equals(response.getStatus)) {
      return response.getResults.get(0).getGeometry.getLocation
    }
    else {
      throw new RuntimeException("could geocode the address" + response.getStatus)
    }
  }
}
