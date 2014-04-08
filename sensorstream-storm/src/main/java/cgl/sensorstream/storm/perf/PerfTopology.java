package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Map;

public class PerfTopology {
    public static class ExclamationBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            tuple.getValue(10);
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("time"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        JmsSpout spout = new JmsSpout();
        spout.setJmsProvider(new JmsProvider() {
            @Override
            public ConnectionFactory connectionFactory() throws Exception {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");

                return connectionFactory;
            }

            @Override
            public Destination destination() throws Exception {

                return null;
            }
        });

        spout.setJmsTupleProducer(new JmsTupleProducer() {
            @Override
            public Values toTuple(Message message) throws JMSException {
                if (message instanceof TextMessage) {
                    long time = ((TextMessage) message).getLongProperty("time");
                    return new Values(System.currentTimeMillis() - time);
                } else {
                    return null;
                }
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
                outputFieldsDeclarer.declare(new Fields("time"));
            }
        });

        builder.setSpout("word", spout, 3);
        builder.setBolt("time", new ExclamationBolt(), 3).shuffleGrouping("time");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(4);
            StormSubmitter.submitTopology("perf", conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("perf", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("perf");
            cluster.shutdown();
        }
    }
}
