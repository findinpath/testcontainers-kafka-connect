package com.findinpath.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.util.HashMap;

import static com.findinpath.testcontainers.Utils.CONFLUENT_PLATFORM_VERSION;
import static com.findinpath.testcontainers.Utils.containerLogsConsumer;
import static com.findinpath.testcontainers.Utils.getRandomFreePort;
import static java.lang.String.format;

/**
 * This class is a testcontainers Confluent implementation for the
 * Apache Kafka docker container.
 */
public class KafkaContainer extends GenericContainer<KafkaContainer> {
    private static final int KAFKA_INTERNAL_PORT = 9092;
    private static final int KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT = 29092;

    private final String networkAlias = "kafka";
    private final int exposedPort;
    private final String bootstrapServersUrl;
    private final String internalBootstrapServersUrl;

    public KafkaContainer(String zookeeperConnect) {
        this(CONFLUENT_PLATFORM_VERSION, zookeeperConnect);
    }

    public KafkaContainer(String confluentPlatformVersion, String zookeeperConnect) {
        super(getKafkaContainerImage(confluentPlatformVersion));

        this.exposedPort = getRandomFreePort();

        var env = new HashMap<String, String>();
        env.put("KAFKA_BROKER_ID", "1");
        env.put("KAFKA_ZOOKEEPER_CONNECT", zookeeperConnect);
        env.put("ZOOKEEPER_SASL_ENABLED", "false");
        env.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        env.put("KAFKA_LISTENERS",
                "PLAINTEXT://0.0.0.0:" + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT +
                ",PLAINTEXT_HOST://0.0.0.0:" + KAFKA_INTERNAL_PORT);
        env.put("KAFKA_ADVERTISED_LISTENERS",
                "PLAINTEXT://" + networkAlias + ":" + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT
                        + ",PLAINTEXT_HOST://" + getContainerIpAddress() + ":" + exposedPort);
        env.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT");
        env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAINTEXT");
        env.put("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT");
        env.put("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAINTEXT");

        withLogConsumer(containerLogsConsumer(logger()));

        withEnv(env);
        withNetworkAliases(networkAlias);
        addFixedExposedPort(exposedPort, KAFKA_INTERNAL_PORT);


        this.bootstrapServersUrl = format("%s:%d", getContainerIpAddress(), exposedPort);
        this.internalBootstrapServersUrl = format("%s:%d", networkAlias, KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT);
    }

    private static String getKafkaContainerImage(String confluentPlatformVersion) {
        return (String) TestcontainersConfiguration
                .getInstance().getProperties().getOrDefault(
                        "kafka.container.image",
                        "confluentinc/cp-kafka:" + confluentPlatformVersion
                );
    }

    /**
     * Get the url.
     *
     * @return
     */
    public String getBootstrapServersUrl() {
        return bootstrapServersUrl;
    }

    /**
     * Get the local url
     *
     * @return
     */
    public String getInternalBootstrapServersUrl() {
        return internalBootstrapServersUrl;
    }
}