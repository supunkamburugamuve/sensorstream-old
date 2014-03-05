package cgl.sensorstream.storm;

import backtype.storm.contrib.jms.DestinationChanger;
import backtype.storm.contrib.jms.Notification;

import java.util.concurrent.BlockingQueue;

public class ZooKeeperDestinationChanger implements DestinationChanger {

    @Override
    public BlockingQueue<Notification> getNotifications() {
        return null;
    }
}
