package com.findinpath.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.TestcontainersConfiguration;

import static com.findinpath.testcontainers.Utils.CONFLUENT_PLATFORM_VERSION;
import static com.findinpath.testcontainers.Utils.containerLogsConsumer;
import static java.lang.String.format;

/**
 * This class is a testcontainers Confluent implementation for the
 * Apache Zookeeper docker container.
 */
public class ZookeeperContainer extends GenericContainer<ZookeeperContainer> {

    private static final int ZOOKEEPER_INTERNAL_PORT = 2181;
    private static final int ZOOKEEPER_TICK_TIME = 2000;

    private final String networkAlias = "zookeeper";

    public ZookeeperContainer() {
        this(CONFLUENT_PLATFORM_VERSION);
    }

    public ZookeeperContainer(String confluentPlatformVersion) {
        super(getZookeeperContainerImage(confluentPlatformVersion));

        addEnv("ZOOKEEPER_CLIENT_PORT", Integer.toString(ZOOKEEPER_INTERNAL_PORT));
        addEnv("ZOOKEEPER_TICK_TIME", Integer.toString(ZOOKEEPER_TICK_TIME));
        withLogConsumer(containerLogsConsumer(logger()));

        addExposedPort(ZOOKEEPER_INTERNAL_PORT);
        withNetworkAliases(networkAlias);
    }

    private static String getZookeeperContainerImage(String confluentPlatformVersion) {
        return (String) TestcontainersConfiguration
                .getInstance().getProperties().getOrDefault(
                        "zookeeper.container.image",
                        "confluentinc/cp-zookeeper:" + confluentPlatformVersion
                );
    }

    /**
     * Get the local url
     *
     * @return
     */
    public String getInternalUrl() {
        return format("%s:%d", networkAlias, ZOOKEEPER_INTERNAL_PORT);
    }
}
