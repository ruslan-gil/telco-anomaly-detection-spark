package com.mapr.cell.common;

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class Config {
    public static final String CONFIG_CONF = "config.conf";
    public static String INIT_TOPIC_NAME = "init";
    public static String MOVE_TOPIC_NAME = "move";


    private Properties properties = new Properties();
    private String streamName;
    private static Config instance;
    private Config() {
        try (InputStream props = Resources.getResource(CONFIG_CONF).openStream()) {
            properties.load(props);
            streamName = properties.getProperty("kafka.streams.consumer.default.stream");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static synchronized Config getConfig() {
        if (instance == null) {
            instance = new Config();
        }
        return instance;
    }

    public HashMap<String, String> getPrefixedMap(String prefix) {
        HashMap<String, String> props = new HashMap<>();
        for (final String name: properties.stringPropertyNames()) {
            if (name.startsWith(prefix)) {
                props.put(name.substring(prefix.length()), properties.getProperty(name));
            }
        }
        return props;
    }

    public Properties getPrefixedProps(String prefix) {
        Properties props = new Properties();
        for (final String name: properties.stringPropertyNames()) {
            if (name.startsWith(prefix)) {
                props.put(name.substring(prefix.length()), properties.getProperty(name));
            }
        }
        return props;
    }

    public Properties getProperties() {
        return properties;
    }

}
