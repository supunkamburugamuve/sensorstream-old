package cgl.sensorstream.core.updates;

import javax.jms.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A listener for getting updates about sensors
 */
public class UpdateListener {
    private static Logger LOG = LoggerFactory.getLogger(UpdateListener.class);

    private ConnectionFactory connectionFactory;

    private Destination listeningDestination;

    private Connection connection;

    private Session session;

    private MessageConsumer consumer;

    private MessageListener messageListener;

    public UpdateListener(ConnectionFactory connectionFactory,
                          Destination listeningDestination, MessageListener messageListener) {
        this.messageListener = messageListener;
        this.connectionFactory = connectionFactory;
        this.listeningDestination = listeningDestination;

        init();
    }

    private void init() {
        try {
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
