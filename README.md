Production/Consumption of Apache Kafka AVRO messages with testcontainers library
=======================================================================================

This is a showcase on how to test the synchronization of the contents
of a Postgres table via [kafka-connect-jdbc](https://docs.confluent.io/current/connect/kafka-connect-jdbc/index.html)
towards [Apache Kafka](https://kafka.apache.org/).

The contents of the input Postgres table are synced in  [AVRO](https://avro.apache.org/) messages 
towards Apache Kafka.
 
The current project makes use of [docker](https://www.docker.com/) containers 
(via [testcontainers](https://www.testcontainers.org/) library) for showcasing
the Confluent Kakfa Connect functionality in an automated test case.

This proof of concept can be used for making end to end system test cases with
[docker](https://www.docker.com/) for architectures that rely on 
[kafka-connect-jdbc](https://docs.confluent.io/current/connect/kafka-connect-jdbc/index.html).

 
The [testcontainers](https://www.testcontainers.org/) library already
offers a [Kafka](https://www.testcontainers.org/modules/kafka/) module
for interacting with [Apache Kafka](https://kafka.apache.org/), but
there is not, at the moment, a testcontainers module for the whole
Confluent environment (Confluent Schema Registry container support is
missing from the module previously mentioned).
As a side note, the containers used do not use the default ports exposed
by default in the artifacts (e.g. : Apache Zookeeper 2181, Apache Kafka 9092,
Confluent Schema Registry 8081), but rather free ports available on the
test machine avoiding therefor possible conflicts with already running
services on the test machine. 

This project provides a functional prototype on how to setup the whole
Confluent environment (including **Confluent Schema Registry** and **Apache Kafka Connect**) 
via testcontainers.

**NOTE** In order to interact with PostgreSQL from **Apache Kafka Connect** the PostgreSQL
driver (see [postgresql-42.2.12.jar](src/test/resources/plugins/kafka-connect-jdbc/postgresql-42.2.12.jar))
is installed in the **Apache Kafka Connect* testcontainer.
See the [KafkaConnectDemoTest.java](src/test/java/com/findinpath/KafkaConnectDemoTest.java) for details on how
to [KafkaConnectContainer](src/test/java/com/findinpath/testcontainers/KafkaConnectContainer.java) is initialized
with this plugin.

More details on installing Apache Kafka Connect plugins are available
 [here](https://docs.confluent.io/current/connect/userguide.html#connect-installing-plugins).
 
 
For the test environment the following containers will be started:
 
- Apache Zookeeper
- Apache Kafka
- Confluent Schema Registry
- Confluent Kafka Connect
- PostgreSQL

It is quite impressive to see how close a productive environment can be simulated in the test cases  
with the [testcontainers](https://www.testcontainers.org/) library.
 
Once the test environment is started, a kafka-connect-jdbc connector will be registered 
for the `bookmarks` PostgreSQL table.
The kafka-connect-jdbc connector will then poll the `bookmarks` table and will sync
its contents towards the `findinpath.bookmarks` Apache Kafka topic.

The test demo verifies whether the dynamically inserted contents 
into the `bookmarks` Postgres table get successfully synced in 
AVRO format on the Apache Kafka topic `findinpath.bookmarks` in the 
same order as they were inserted. 

Use 

```bash
mvn clean install
```

for trying out the project.

The file [testcontainers.properties](src/test/resources/testcontainers.properties) can be
used for overriding the default [docker](https://www.docker.com/) images used for the containers needed in setting 
up the Confluent test environment.
