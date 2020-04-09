package com.findinpath.kafkaconnect;

import java.util.HashMap;
import java.util.Map;

/**
 * This class models the kafka-conect connector configuration
 * that is used when registering/updating Kafka Connect connectors.
 */
public class ConnectorConfiguration {

    /**
     * Connector name
     */
    private String name;

    /**
     * Connector configuration
     */
    private Map<String, String> config = new HashMap<>();


    public ConnectorConfiguration(String name, Map<String, String> config) {
        this.name = name;
        this.config = config;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public String toString() {
        return "ConnectorConfiguration{" +
                "name='" + name + '\'' +
                ", config=" + config +
                '}';
    }
}