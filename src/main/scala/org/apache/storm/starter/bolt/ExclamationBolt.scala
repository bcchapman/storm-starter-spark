package org.apache.storm.starter.bolt

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

class ExclamationBolt extends BaseRichBolt {
  var _collector: OutputCollector = _

  override def prepare(conf: java.util.Map[_, _], context: TopologyContext, collector: OutputCollector) {
    _collector = collector
  }

  override def execute(tuple: Tuple) {
    _collector.emit(tuple, new Values(exclaimString(tuple.getString(0))))
    _collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("word"))
  }

  def exclaimString(str: String) = str.concat("!!!")
}
