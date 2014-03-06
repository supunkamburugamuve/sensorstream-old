package cgl.sensorstream.storm;

import backtype.storm.contrib.jms.DestinationChanger;
import backtype.storm.contrib.jms.Notification;
import cgl.sensorstream.core.Utils;
import cgl.sensorstream.core.config.Configuration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ZooKeeperDestinationChanger implements DestinationChanger {
    private static Logger LOG = LoggerFactory.getLogger(ZooKeeperDestinationChanger.class);

    private BlockingQueue<Notification> notifications;

    private CuratorFramework client = null;
    private PathChildrenCache cache = null;

    public void start(Map conf) {
        notifications = new ArrayBlockingQueue<Notification>((Integer)
                conf.get(Configuration.SS_SENSOR_UPDATES_SIZE));

        client = CuratorFrameworkFactory.newClient(Utils.getZkConnectionString(conf),
                new ExponentialBackoffRetry(1000, 3));
        client.start();

        // in this example we will cache data. Notice that this is optional.
        cache = new PathChildrenCache(client, Configuration.getZkRoot(conf), true);
        try {
            cache.start();
        } catch (Exception e) {
            String msg = "Failed to start the path cache";
            LOG.error(msg, e);
            throw new RuntimeException(msg, e);
        }

        addListener(cache);
    }

    private static void addListener(PathChildrenCache cache) {
        // a PathChildrenCacheListener is optional. Here, it's used just to log changes
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED: {
                        System.out.println("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                    case CHILD_UPDATED: {
                        System.out.println("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                    case CHILD_REMOVED: {
                        System.out.println("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    @Override
    public BlockingQueue<Notification> getNotifications() {
        return notifications;
    }

    public void stop() {
        CloseableUtils.closeQuietly(cache);
        CloseableUtils.closeQuietly(client);
    }
}
