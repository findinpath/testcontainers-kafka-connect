package com.findinpath;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.findinpath.kafkaconnect.ConnectorConfiguration;
import com.findinpath.testcontainers.KafkaConnectContainer;
import com.findinpath.testcontainers.KafkaContainer;
import com.findinpath.testcontainers.SchemaRegistryContainer;
import com.findinpath.testcontainers.ZookeeperContainer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.restassured.http.ContentType;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * This is a showcase on how to test the synchronization of the contents of
 * a Postgres database table via Confluent Kafka ecosystem with the
 * help of Docker containers (via
 * <a href="https://www.testcontainers.org/>testcontainers</a> library).
 * <p>
 * For the test environment the following containers will be started:
 *
 * <ul>
 *   <li>Apache Zookeeper</li>
 *   <li>Apache Kafka</li>
 *   <li>Confluent Schema Registry</li>
 *   <li>Confluent Kafka Connect</li>
 *   <li>PostgreSQL</li>
 * </ul>
 * <p>
 * Once the test environment is started, a kafka-connect-jdbc
 * connector will be registered for the <code>bookmarks</code>
 * PostgreSQL table.
 * The kafka-connect-jdbc connector will then poll the
 * <code>bookmarks</code> table and will sync
 * its contents towards the <code>findinpath.bookmarks</code>
 * Apache Kafka topic.
 * <p>
 * The {@link #demo()} test will  verify whether
 * the dynamically inserted contents into the <code>bookmarks</code>
 * Postgres table get successfully synced in AVRO format on the Apache Kafka
 * topic <code>findinpath.bookmarks</code> in the same order as they were inserted.
 */
public class KafkaConnectDemoTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectDemoTest.class);
    private static final long POLL_INTERVAL_MS = 100L;
    private static final long POLL_TIMEOUT_MS = 20_000L;

    private static final String KAFKA_CONNECT_CONNECTOR_NAME = "bookmarks";
    private static final String KAFKA_CONNECT_OUTPUT_TOPIC = "findinpath.bookmarks";


    private static final String POSTGRES_DB_NAME = "findinpath";
    private static final String POSTGRES_NETWORK_ALIAS = "postgres";
    private static final String POSTGRES_DB_USERNAME = "sa";
    private static final String POSTGRES_DB_PASSWORD = "p@ssw0rd!";
    private static final int POSTGRES_INTERNAL_PORT = 5432;
    private static final String POSTGRES_CONNECTION_URL = String.format("jdbc:postgresql://%s:%d/%s?loggerLevel=OFF",
            POSTGRES_NETWORK_ALIAS,
            POSTGRES_INTERNAL_PORT,
            POSTGRES_DB_NAME);

    private static Network network;

    private static ZookeeperContainer zookeeperContainer;
    private static KafkaContainer kafkaContainer;
    private static SchemaRegistryContainer schemaRegistryContainer;
    private static KafkaConnectContainer kafkaConnectContainer;

    private static PostgreSQLContainer postgreSQLContainer;

    private static final ObjectMapper mapper = new ObjectMapper();


    /**
     * Bootstrap the docker instances needed for interracting with :
     * <ul>
     *     <li>Confluent Kafka ecosystem</li>
     *     <li>PostgreSQL</li>
     * </ul>
     */
    @BeforeAll
    public static void dockerSetup() throws Exception {
        network = Network.newNetwork();

        // Confluent setup
        zookeeperContainer = new ZookeeperContainer()
                .withNetwork(network);
        kafkaContainer = new KafkaContainer(zookeeperContainer.getInternalUrl())
                .withNetwork(network);
        schemaRegistryContainer = new SchemaRegistryContainer(zookeeperContainer.getInternalUrl())
                .withNetwork(network);
        kafkaConnectContainer = new KafkaConnectContainer(kafkaContainer.getInternalBootstrapServersUrl())
                .withNetwork(network)
                .withPlugins("plugins/kafka-connect-jdbc/postgresql-42.2.12.jar")
                .withKeyConverter("org.apache.kafka.connect.storage.StringConverter")
                .withValueConverter("io.confluent.connect.avro.AvroConverter")
                .withSchemaRegistryUrl(schemaRegistryContainer.getInternalUrl());

        // Postgres setup
        postgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
                .withNetwork(network)
                .withNetworkAliases(POSTGRES_NETWORK_ALIAS)
                .withInitScript("postgres/init_postgres.sql")
                .withDatabaseName(POSTGRES_DB_NAME)
                .withUsername(POSTGRES_DB_USERNAME)
                .withPassword(POSTGRES_DB_PASSWORD);

        Startables
                .deepStart(Stream.of(zookeeperContainer,
                        kafkaContainer,
                        schemaRegistryContainer,
                        kafkaConnectContainer,
                        postgreSQLContainer)
                )
                .join();


        verifyKafkaConnectHealth();
    }


    @Test
    public void demo() {

        var bookmarksTableConnectorConfig = createBookmarksTableConnectorConfig(KAFKA_CONNECT_CONNECTOR_NAME,
                POSTGRES_CONNECTION_URL,
                POSTGRES_DB_USERNAME,
                POSTGRES_DB_PASSWORD);
        registerBookmarksTableConnector(bookmarksTableConnectorConfig);

        var findinpathBookmark = new Bookmark();
        findinpathBookmark.setName("findinpath");
        findinpathBookmark.setUrl("https://www.findinpath.com");
        var findinpathBookmarkId = insertBookmark(findinpathBookmark);

        var twitterBookmark = new Bookmark();
        twitterBookmark.setName("twitter");
        twitterBookmark.setUrl("https://www.twitter.com");
        var twitterBookmarkId = insertBookmark(twitterBookmark);

        awaitForTopicCreation(KAFKA_CONNECT_OUTPUT_TOPIC, 10, TimeUnit.SECONDS);


        var genericRecords = dumpGenericRecordTopic(KAFKA_CONNECT_OUTPUT_TOPIC, 2, POLL_TIMEOUT_MS);
        LOGGER.info(String.format("Retrieved %d consumer records from the topic %s",
                genericRecords.size(), KAFKA_CONNECT_OUTPUT_TOPIC));


        assertThat(genericRecords.size(), equalTo(2));
        // the Kafka JBDC connector does not generate the key by default.
        assertThat(genericRecords.get(0).value().get("id"),
                equalTo(findinpathBookmarkId));
        assertThat(genericRecords.get(0).value().get("name").toString(), equalTo(findinpathBookmark.getName()));
        assertThat(genericRecords.get(0).value().get("url").toString(), equalTo(findinpathBookmark.getUrl()));

        assertThat(genericRecords.get(1).value().get("id"),
                equalTo(twitterBookmarkId));
        assertThat(genericRecords.get(1).value().get("name").toString(), equalTo(twitterBookmark.getName()));
        assertThat(genericRecords.get(1).value().get("url").toString(), equalTo(twitterBookmark.getUrl()));
    }


    /**
     * Retrieves a PostgreSQL database connection
     *
     * @return a database connection
     * @throws SQLException wraps the exceptions which may occur
     */
    public Connection connect() throws SQLException {
        return DriverManager.getConnection(postgreSQLContainer.getJdbcUrl(), POSTGRES_DB_USERNAME, POSTGRES_DB_PASSWORD);
    }

    /**
     * Wraps the JDBC complexity needed for inserting into PostgreSQL a new Bookmark entry.
     *
     * @param bookmark the bookmark to be inserted.
     * @return the ID of the inserted bookmark in the database.
     */
    public long insertBookmark(Bookmark bookmark) {
        String SQL = "INSERT INTO bookmarks(name,url) VALUES (?,?)";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL,
                     Statement.RETURN_GENERATED_KEYS)) {

            pstmt.setString(1, bookmark.getName());
            pstmt.setString(2, bookmark.getUrl());

            int affectedRows = pstmt.executeUpdate();
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    if (rs.next()) {
                        return rs.getLong(1);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("Exception occurred while inserting " + bookmark, e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Exception occurred while inserting " + bookmark, e);
        }

        return 0;
    }


    private static String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static ConnectorConfiguration createBookmarksTableConnectorConfig(String connectorName,
                                                                              String connectionUrl,
                                                                              String connectionUser,
                                                                              String connectionPassword) {

        var config = new HashMap<String, String>();
        config.put("name", connectorName);
        config.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        config.put("tasks.max", "1");
        config.put("connection.url", connectionUrl);
        config.put("connection.user", connectionUser);
        config.put("connection.password", connectionPassword);
        config.put("table.whitelist", "bookmarks");
        config.put("mode", "timestamp+incrementing");
        config.put("timestamp.column.name", "updated");
        config.put("validate.non.null", "false");
        config.put("incrementing.column.name", "id");
        config.put("topic.prefix", "findinpath.");

        return new ConnectorConfiguration(connectorName, config);
    }

    private static void registerBookmarksTableConnector(ConnectorConfiguration bookmarksTableConnectorConfig) {
        given()
                .log().all()
                .contentType(ContentType.JSON)
                .accept(ContentType.JSON)
                .body(toJson(bookmarksTableConnectorConfig))
                .when()
                .post(kafkaConnectContainer.getUrl() + "/connectors")
                .andReturn()
                .then()
                .log().all()
                .statusCode(HttpStatus.SC_CREATED);
    }


    /**
     * Simple HTTP check to see that the kafka-connect server is available.
     */
    private static void verifyKafkaConnectHealth() {
        given()
                .log().headers()
                .contentType(ContentType.JSON)
                .when()
                .get(kafkaConnectContainer.getUrl())
                .andReturn()
                .then()
                .log().all()
                .statusCode(HttpStatus.SC_OK);
    }


    /**
     * Wait at most until the timeout, for the creation of the specified <code>topicName</code>.
     *
     * @param topicName the Apache Kafka topic name for which it is expected the creation
     * @param timeout   the amount of time units to expect until the operation is ca
     * @param timeUnit  the time unit a timeout exception will be thrown
     */
    private static void awaitForTopicCreation(String topicName, long timeout, TimeUnit timeUnit) {
        try (var adminClient = createAdminClient()) {
            await()
                    .atMost(timeout, timeUnit)
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> adminClient.listTopics().names().get().contains(topicName));
        }
    }

    private static <T extends GenericRecord> KafkaConsumer<String, T> createGenericRecordKafkaConsumer(
            String consumerGroupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServersUrl());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryContainer.getUrl());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        props.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
                TopicNameStrategy.class.getName());
        return new KafkaConsumer<>(props);
    }

    /**
     * Utility method for dumping the AVRO contents of a Apache Kafka topic.
     *
     * @param topic             the topic name
     * @param minMessageCount   the amount of messages to wait for before completing the operation
     * @param pollTimeoutMillis the period to wait until throwing a timeout exception
     * @param <T>
     * @return the generic records contained in the specified topic.
     */
    private <T extends GenericRecord> List<ConsumerRecord<String, T>> dumpGenericRecordTopic(
            String topic,
            int minMessageCount,
            long pollTimeoutMillis) {
        List<ConsumerRecord<String, T>> consumerRecords = new ArrayList<>();
        var consumerGroupId = UUID.randomUUID().toString();
        try (final KafkaConsumer<String, T> consumer = createGenericRecordKafkaConsumer(
                consumerGroupId)) {
            // assign the consumer to all the partitions of the topic
            var topicPartitions = consumer.partitionsFor(topic).stream()
                    .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                    .collect(Collectors.toList());
            consumer.assign(topicPartitions);
            var start = System.currentTimeMillis();
            while (true) {
                final ConsumerRecords<String, T> records = consumer
                        .poll(Duration.ofMillis(POLL_INTERVAL_MS));
                records.forEach(consumerRecords::add);
                if (consumerRecords.size() >= minMessageCount) {
                    break;
                }
                if (System.currentTimeMillis() - start > pollTimeoutMillis) {
                    throw new IllegalStateException(
                            String.format(
                                    "Timed out while waiting for %d messages from the %s. Only %d messages received so far.",
                                    minMessageCount, topic, consumerRecords.size()));
                }
            }
        }
        return consumerRecords;
    }

    /**
     * Creates a utility class for interacting for administrative purposes with Apache Kafka.
     *
     * @return the admin client.
     */
    private static AdminClient createAdminClient() {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServersUrl());

        return KafkaAdminClient.create(properties);
    }


}
