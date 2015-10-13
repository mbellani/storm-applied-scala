package com.storm.getting_started

import backtype.storm.utils.Utils
import backtype.storm.{LocalCluster, Config}
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields

object LocalTopologyRunner {

  val TEN_MINUTES = 600000;

  def main(args: Array[String]) {
    val topologyBuilder = new TopologyBuilder()
    topologyBuilder.setSpout("commit-feed-listener", new CommitFeedListener)
    topologyBuilder.setBolt("email-extractor", new EmailExtractor)
      .shuffleGrouping("commit-feed-listener")
    topologyBuilder.setBolt("email-counter", new EmailCounter)
      .fieldsGrouping("email-extractor", new Fields("email"))

    val config = new Config
    val topology = topologyBuilder.createTopology()
    val cluster = new LocalCluster()

    config.setDebug(true)
    cluster.submitTopology("github-commit-count-topology", config, topology)

    Utils.sleep(TEN_MINUTES)

    cluster.killTopology("github-commit-count-topology")
    cluster.shutdown()
  }
}
