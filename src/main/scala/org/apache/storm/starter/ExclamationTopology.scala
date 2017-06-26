package org.apache.storm.starter

import org.apache.storm.{Config, LocalCluster}
import org.apache.storm.starter.bolt.ExclamationBolt
import org.apache.storm.testing.TestWordSpout
import org.apache.storm.topology.TopologyBuilder

object ExclamationTopology {

  def main(args: Array[String]) {
    val builder = new TopologyBuilder()

    builder.setSpout("word", new TestWordSpout(), 10)
    builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word")
    builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1")

    val conf = new Config
    conf.setDebug(true)
    conf.setNumWorkers(3)

    var topologyName = "test"
    if (args != null && args.length > 0) {
      topologyName = args(0)
    }

    val cluster = new LocalCluster();
    cluster.submitTopology(topologyName, conf, builder.createTopology())

  }
}
