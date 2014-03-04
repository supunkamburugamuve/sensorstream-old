package cgl.sensorstream.core.config;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;

public class Configuration {
    public static final String DEFAULT_CONFIG_FILE = "sensorstream.yaml";

    private String fileName;

    private Map map;

    public Configuration() throws ConfigurationException {
        this(DEFAULT_CONFIG_FILE);
    }

    public Configuration(String fileName) throws ConfigurationException {
        this.fileName = fileName;
        init();
    }

    private void init() throws ConfigurationException {
        Object config = null;

        map = findAndReadConfigFile(fileName, true);
    }

    public static Map findAndReadConfigFile(String name, boolean mustExist) {
        try {
            HashSet<URL> resources = new HashSet<URL>(findResources(name));
            if(resources.isEmpty()) {
                if(mustExist) throw new RuntimeException("Could not find config file on classpath " + name);
                else return new HashMap();
            }
            if(resources.size() > 1) {
                throw new RuntimeException("Found multiple " + name + " resources. You're probably bundling the Storm jars with your topology jar. "
                        + resources);
            }
            URL resource = resources.iterator().next();
            Map ret = (Map) Yaml.load(new InputStreamReader(resource.openStream()));

            if(ret==null) ret = new HashMap();

            return new HashMap(ret);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL> ret = new ArrayList<URL>();
            while(resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
