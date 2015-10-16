package stormbook

import java.util.concurrent.TimeUnit

import backtype.storm.utils.Utils
import backtype.storm.{Config, LocalCluster}
import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder

object Builder {

  def build(): StormTopology = {
    val topologyBuilder = new TopologyBuilder
    topologyBuilder.setSpout("orders", new RabbitMQSpout, 2)
    topologyBuilder.setBolt("authorize-credit-card", new AuthorizeCreditCard, 2)
      .shuffleGrouping("orders")
    topologyBuilder.setBolt("notifier", new ProcessedOrderNotification, 2)
      .shuffleGrouping("authorize-credit-card")
    topologyBuilder.createTopology()
  }

}

object Runner {
  def main(args: Array[String]): Unit = {
    val localCluster = new LocalCluster()
    val config = new Config
    localCluster.submitTopology("credit-card-authorizer", config, Builder.build())
    Utils.sleep(TimeUnit.MINUTES.toMillis(10))
    localCluster.killTopology("credit-card-authorizer")
    localCluster.shutdown()
  }
}
