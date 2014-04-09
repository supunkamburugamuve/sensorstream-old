package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class PerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        JmsSpout spout = new JmsSpout();
        spout.setJmsProvider(new LocalJMSProvider());

        spout.setJmsTupleProducer(new JmsTupleProducer() {
            @Override
            public Values toTuple(Message message) throws JMSException {
                if (message instanceof TextMessage) {
                    long time = message.getLongProperty("time");
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

        builder.setSpout("word", spout, 4);
        builder.setBolt("time1", new PerfAggrBolt(), 4).shuffleGrouping("word");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(6);
            StormSubmitter.submitTopology("perf", conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("perf", conf, builder.createTopology());
            Thread.sleep(60000);
            cluster.killTopology("perf");
            cluster.shutdown();
        }
    }

    private static class LocalJMSProvider implements JmsProvider {
        ConnectionFactory connectionFactory;
        Destination destination;

        private LocalJMSProvider() {
            this.connectionFactory = new ActiveMQConnectionFactory("tcp://10.39.1.55:61616");
            Connection connection;
            try {
                connection = connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                this.destination = session.createQueue("send");
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        @Override
        public ConnectionFactory connectionFactory() throws Exception {
            return connectionFactory;
        }

        @Override
        public Destination destination() throws Exception {
            return destination;
        }
    }
}
