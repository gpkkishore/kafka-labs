-- JDBC sink target. Tables are auto-created by the sink connectors (auto.create / schema.evolution).
CREATE SCHEMA IF NOT EXISTS replica;   -- used by 16-cdc-debezium-mysql (Debezium JDBC sink)
