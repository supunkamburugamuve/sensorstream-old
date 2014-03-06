package cgl.sensorstream.core;

import java.util.Map;

public class SensorStreamMain {

    public static void main(String[] args) {
        // read the configuration
        Map conf = Utils.readDefaultConfig();

        // now try to connect to zookeeper
        ZooKeeperUpdater zkUpdater = new ZooKeeperUpdater(conf);
        zkUpdater.start();

    }
}
