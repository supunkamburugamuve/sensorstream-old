package cgl.sensorstream.core.updates;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Map;

/**
 * A listener for getting updates about sensors
 */
public class UpdateListener {
    private static Logger LOG = LoggerFactory.getLogger(UpdateListener.class);

    // listening destination
    private Destination listeningDestination;

    // the connection
    private Connection connection;

    // jms session
    private Session session;

    // message consumer
    private MessageConsumer consumer;

    // the jms listener
    private MessageListener messageListener;

    public UpdateListener(Map conf) {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();

            // Create a Connection
            connection = connectionFactory.createConnection();
            connection.start();

            connection.setExceptionListener(new ListeningException());
            // Create a Session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // Create a MessageConsumer from the Session to the Topic or Queue
            consumer = session.createConsumer(listeningDestination);
            // register a handler for receiving messages
            consumer.setMessageListener(messageListener);
        } catch (Exception e) {
            String s = "Failed to create the JMS connection";
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    private String getJMSConnectionString(Map conf) {
        return conf.get("");
    }

    public void destroy() {
        try {
            consumer.close();
            session.close();
            connection.close();
        } catch (JMSException e) {
            LOG.error("Failed to close the JMS Connection", e);
        }
    }

    private class ListeningException implements ExceptionListener {
        @Override
        public void onException(JMSException e) {
            LOG.error("Exception occurred in JMS Connection", e);
        }
    }
}
