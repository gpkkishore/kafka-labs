# Flink 1.20 + the Kafka SQL connector (the "fat" sql-connector jar bundles the Kafka client).
# The json / debezium-json / canal-json formats ship with Flink (lib/flink-json-*.jar).
FROM flink:1.20.0-scala_2.12-java11
ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.20
RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"
# checkpoint dir (a named volume mounted here inherits this ownership on first use)
RUN mkdir -p /tmp/flink-checkpoints && chown flink:flink /tmp/flink-checkpoints
