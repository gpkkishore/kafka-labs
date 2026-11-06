-- =====================================================================================
-- Table definitions shared by all jobs (loaded with `sql-client.sh -i`).
-- A Flink "table" here is only METADATA describing a Kafka topic: no data is copied.
-- =====================================================================================

-- 1) Click stream produced by 01_clickstream_producer.py (append-only JSON events)
CREATE TABLE clicks (
  user_id     INT,
  product_id  INT,
  action      STRING,                        -- view | add_to_cart | purchase
  quantity    INT,
  ts_ms       BIGINT,                        -- event time set by the producer (may be late / out of order)
  event_time  AS TO_TIMESTAMP_LTZ(ts_ms, 3), -- computed column
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  -- WATERMARK = "I believe no event older than (max seen event_time - 5s) will still arrive".
  -- Windows close when the watermark passes their end; later events are dropped.
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'kinaction_flink_clicks',
  'properties.bootstrap.servers' = 'kafka1:29092,kafka2:29092,kafka3:29092',
  'properties.group.id' = 'flink-clicks',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- 2) Product table from Debezium CDC (16-cdc-debezium-mysql). debezium-json turns the envelope into a
--    CHANGELOG: op=c/r -> +I, u -> -U/+U, d -> -D. Flink keeps it as a continuously updated table.
CREATE TABLE products (
  id        INT,
  name      STRING,
  category  STRING,
  price     DOUBLE,
  stock     INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.inventory.products',
  'properties.bootstrap.servers' = 'kafka1:29092,kafka2:29092,kafka3:29092',
  'properties.group.id' = 'flink-products',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json',
  'debezium-json.schema-include' = 'true'    -- the connector uses JsonConverter with schemas.enable=true
);

-- 3) Sinks ---------------------------------------------------------------------------
-- Append-only results (window TVF outputs never change once emitted) -> plain kafka + json
CREATE TABLE action_counts_10s (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  action       STRING,
  event_count  BIGINT,
  user_count   BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'kinaction_flink_action_counts',
  'properties.bootstrap.servers' = 'kafka1:29092,kafka2:29092,kafka3:29092',
  'format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once'
);

CREATE TABLE trending_products (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  product_id   INT,
  view_count   BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'kinaction_flink_trending',
  'properties.bootstrap.servers' = 'kafka1:29092,kafka2:29092,kafka3:29092',
  'format' = 'json'
);

-- Updating results (a value per key that changes over time) -> upsert-kafka:
-- the PRIMARY KEY becomes the Kafka key, an update is a new record with the same key,
-- a delete is a tombstone. Perfect with a compacted topic.
CREATE TABLE category_revenue (
  category   STRING,
  purchases  BIGINT,
  revenue    DOUBLE,
  PRIMARY KEY (category) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'kinaction_flink_category_revenue',
  'properties.bootstrap.servers' = 'kafka1:29092,kafka2:29092,kafka3:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE inventory_value (
  category       STRING,
  product_count  BIGINT,
  units_in_stock BIGINT,
  stock_value    DOUBLE,
  PRIMARY KEY (category) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'kinaction_flink_inventory_value',
  'properties.bootstrap.servers' = 'kafka1:29092,kafka2:29092,kafka3:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);
