package com.findinpath.testcontainers;


import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.TestcontainersConfiguration;

import static com.findinpath.testcontainers.Utils.CONFLUENT_PLATFORM_VERSION;
import static com.findinpath.testcontainers.Utils.containerLogsConsumer;
import static java.lang.String.format;

/**
 * This class is a testcontainers implementation for the
 * <a href="Confluent Schema Registry">https://docs.confluent.io/current/schema-registry/index.html</a>
 * Docker container.
 */
public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;


    private final String networkAlias = "schema-registry";

    public SchemaRegistryContainer(String zookeeperConnect) {
        this(CONFLUENT_PLATFORM_VERSION, zookeeperConnect);
    }

    public SchemaRegistryContainer(String confluentPlatformVersion, String zookeeperUrl) {
        super(getSchemaRegistryContainerImage(confluentPlatformVersion));

        addEnv("SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL", zookeeperUrl);
        addEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");
        withLogConsumer(containerLogsConsumer(logger()));
        withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);
        withNetworkAliases(networkAlias);

        waitingFor(Wait.forHttp("/subjects"));

    }

    private static String getSchemaRegistryContainerImage(String confluentPlatformVersion) {
        return (String) TestcontainersConfiguration
                .getInstance().getProperties().getOrDefault(
                        "schemaregistry.container.image",
                        "confluentinc/cp-schema-registry:" + confluentPlatformVersion
                );
    }

    /**
     * Get the url.
     *
     * @return
     */
    public String getUrl() {
        return format("http://%s:%d", this.getContainerIpAddress(), this.getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT));
    }

    /**
     * Get the local url
     *
     * @return
     */
    public String getInternalUrl() {
        return format("http://%s:%d", networkAlias, SCHEMA_REGISTRY_INTERNAL_PORT);
    }
}

