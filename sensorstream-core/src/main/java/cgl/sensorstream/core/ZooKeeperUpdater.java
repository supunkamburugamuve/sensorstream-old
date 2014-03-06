package cgl.sensorstream.core;

import cgl.sensorstream.core.config.Configuration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import javax.management.remote.rmi.RMIServer;
import java.util.List;
import java.util.Map;

public class ZooKeeperUpdater {
    private CuratorFramework client = null;
    private PathChildrenCache cache = null;

    public void start(Map conf) {
        int port = (int) conf.get(Configuration.SS_ZOOKEEPER_PORT);
        List servers = (List) conf.get(Configuration.SS_ZOOKEEPER_SERVERS);

        CuratorFramework client = null;
        PathChildrenCache cache = null;
        try
        {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            client.start();

            // in this example we will cache data. Notice that this is optional.
            cache = new PathChildrenCache(client, PATH, true);
            cache.start();

            processCommands(client, cache);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(server);
        }
    }

    
}
