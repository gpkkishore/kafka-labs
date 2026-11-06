# Kafka Connect worker image used by 15-kafka-connect AND 16-cdc-debezium-mysql.
#
# Base: Debezium's Connect image (Apache Kafka 3.7 + Debezium 2.7 connectors:
#       MySQL/Postgres/... source connectors, Debezium JDBC *sink* connector, Outbox router SMT).
# We add:
#   * FileStream source/sink  (ships in /kafka/libs but is NOT on the plugin path since Kafka 3.2)
#   * Confluent JDBC connector (JDBC *source* with incrementing/timestamp modes) + MySQL & Postgres drivers
#   * Prometheus JMX java agent (worker / connector / task metrics on :7072)
FROM quay.io/debezium/connect:2.7.3.Final

ARG JDBC_CONNECTOR_VERSION=10.7.6
ARG MYSQL_DRIVER_VERSION=8.3.0
ARG PG_DRIVER_VERSION=42.7.4
ARG JMX_AGENT_VERSION=1.0.1

USER root
RUN set -eux; \
    mkdir -p /kafka/connect/file-connector /kafka/connect/confluent-jdbc /kafka/jmx; \
    cp /kafka/libs/connect-file-*.jar /kafka/connect/file-connector/; \
    cd /tmp; \
    curl -fsSL -o jdbc.zip "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/versions/${JDBC_CONNECTOR_VERSION}/confluentinc-kafka-connect-jdbc-${JDBC_CONNECTOR_VERSION}.zip"; \
    unzip -q jdbc.zip; \
    cp confluentinc-kafka-connect-jdbc-${JDBC_CONNECTOR_VERSION}/lib/*.jar /kafka/connect/confluent-jdbc/; \
    rm -rf jdbc.zip confluentinc-kafka-connect-jdbc-*; \
    curl -fsSL -o /kafka/connect/confluent-jdbc/mysql-connector-j.jar "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_DRIVER_VERSION}/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar"; \
    curl -fsSL -o /kafka/connect/confluent-jdbc/postgresql.jar "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_DRIVER_VERSION}/postgresql-${PG_DRIVER_VERSION}.jar"; \
    curl -fsSL -o /kafka/jmx/jmx_prometheus_javaagent.jar "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_AGENT_VERSION}.jar"; \
    chown -R kafka:kafka /kafka/connect /kafka/jmx
COPY connect-jmx.yml /kafka/jmx/connect-jmx.yml
USER kafka
