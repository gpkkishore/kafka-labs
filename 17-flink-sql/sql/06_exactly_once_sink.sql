-- EXACTLY-ONCE end to end: Kafka source offsets are stored in Flink checkpoints, and the Kafka sink writes
-- inside a Kafka TRANSACTION that is committed only when the checkpoint completes (two-phase commit).
--   * Consumers of the output must use isolation.level=read_committed, otherwise they also see data
--     from aborted/open transactions.
--   * Output becomes visible only every checkpoint interval (10s here) -> latency vs. guarantee trade-off.
--   * transaction.timeout.ms must be <= the broker's transaction.max.timeout.ms (15 min default);
--     Flink's default (1h) would be rejected.
SET 'pipeline.name' = 'exactly-once-purchases';

CREATE TABLE purchases_eos (
  user_id    INT,
  product_id INT,
  quantity   INT,
  event_time TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'kinaction_flink_purchases_eos',
  'properties.bootstrap.servers' = 'kafka1:29092,kafka2:29092,kafka3:29092',
  'format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'flink-purchases-eos',
  'properties.transaction.timeout.ms' = '600000'
);

INSERT INTO purchases_eos
SELECT user_id, product_id, quantity, event_time FROM clicks WHERE action = 'purchase';
