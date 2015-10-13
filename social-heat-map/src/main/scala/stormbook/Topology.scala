package stormbook

import java.util.concurrent.TimeUnit

import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.Utils
import backtype.storm.{Config, LocalCluster}

object HeatmapTopologyBuilder {
  def build(): StormTopology = {
    val topologyBuilder = new TopologyBuilder

    topologyBuilder.setSpout("checkins", new Checkins, 4)
    topologyBuilder.setBolt("geo-code-lookup", new GeocodeLookup, 4)
      .shuffleGrouping("checkins")
    topologyBuilder.setBolt("heatmap-builder", new HeatmapBuilder)
      .globalGrouping("geo-code-lookup")
    topologyBuilder.setBolt("persistor", new Persistor)
      .shuffleGrouping("heatmap-builder")

    topologyBuilder.createTopology()
  }

}

object ToplogyRunner {
  def main(args: Array[String]): Unit = {
    val localCluster = new LocalCluster()
    val config = new Config
    localCluster.submitTopology("social-heat-map", config, HeatmapTopologyBuilder.build())
    Utils.sleep(TimeUnit.MINUTES.toMillis(10))
    localCluster.killTopology("social-heat-map")
    localCluster.shutdown()
  }

}
