package cgl.sensorstream.core;

import cgl.sensorstream.core.config.Configuration;

import java.util.List;
import java.util.Map;

public class ZooKeeperUpdater {
    public void start(Map conf) {
        int port = (int) conf.get(Configuration.SS_ZOOKEEPER_PORT);
        List servers = (List) conf.get(Configuration.SS_ZOOKEEPER_SERVERS);

        CuratorFramework    client = null;
        PathChildrenCache   cache = null;
        try
        {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            client.start();

            // in this example we will cache data. Notice that this is optional.
            cache = new PathChildrenCache(client, PATH, true);
            cache.start();

            processCommands(client, cache);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(server);
        }
    }
}
