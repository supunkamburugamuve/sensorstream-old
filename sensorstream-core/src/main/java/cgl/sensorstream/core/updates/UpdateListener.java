package cgl.sensorstream.core.updates;

import org.apache.activemq.spring.ActiveMQConnectionFactory;

import javax.jms.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateListener {
    private static Logger LOG = LoggerFactory.getLogger(UpdateListener.class);

    private ConnectionFactory connectionFactory;

    private Destination listeningDestination;

    private Connection connection;

    private Session session;

    private MessageConsumer consumer;

    public UpdateListener(ConnectionFactory connectionFactory, Destination listeningDestination) {
        this.connectionFactory = connectionFactory;
        this.listeningDestination = listeningDestination;

        init();
    }

    private void init() {
        try {
            // Create a ConnectionFactory
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
            consumer.setMessageListener(new UpdateMessageListener());
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

    private class UpdateMessageListener implements MessageListener {
        @Override
        public void onMessage(Message message) {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                String text = null;
                try {
                    text = textMessage.getText();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
                System.out.println("Received: " + text);
            } else {
                System.out.println("Received: " + message);
            }
        }
    }

    private class ListeningException implements ExceptionListener {
        @Override
        public void onException(JMSException e) {
            LOG.error("Exception occurred in JMS Connection", e);
        }
    }
}
