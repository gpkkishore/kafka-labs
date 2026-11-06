-- ksqlDB walkthrough (KiA 12.2, AKiA 12.3). Needs 00_clickstream_producer.py running.
--   docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
--   ksql> RUN SCRIPT '/ksql/queries.sql';      -- or paste statement by statement
SET 'auto.offset.reset' = 'earliest';

-- 1) STREAM over an existing topic: unbounded sequence of immutable events
CREATE STREAM IF NOT EXISTS pageviews (
    user_id VARCHAR KEY,
    page    VARCHAR,
    action  VARCHAR,
    ts      BIGINT
) WITH (KAFKA_TOPIC = 'sp_pageviews', VALUE_FORMAT = 'JSON', TIMESTAMP = 'ts');   -- event time from the payload

-- 2) TABLE over a compacted topic: latest value per key
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR PRIMARY KEY,
    country VARCHAR,
    tier    VARCHAR
) WITH (KAFKA_TOPIC = 'sp_users', VALUE_FORMAT = 'JSON');

-- 3) Stateless: filter + projection -> new topic SP_KSQL_PURCHASES (persistent query)
CREATE STREAM IF NOT EXISTS purchases WITH (KAFKA_TOPIC = 'sp_ksql_purchases') AS
    SELECT user_id, page, TIMESTAMPTOSTRING(ts, 'HH:mm:ss') AS event_time
    FROM pageviews
    WHERE action = 'purchase'
    EMIT CHANGES;

-- 4) Stream-table join: enrich each view with the user's current country / tier
CREATE STREAM IF NOT EXISTS pageviews_enriched WITH (KAFKA_TOPIC = 'sp_ksql_pageviews_enriched') AS
    SELECT p.user_id AS user_id, p.page, p.action, u.country, u.tier
    FROM pageviews p
    LEFT JOIN users u ON p.user_id = u.user_id
    EMIT CHANGES;

-- 5) Tumbling window aggregation with grace period (late events up to 10s are still counted)
CREATE TABLE IF NOT EXISTS views_per_page_30s WITH (KAFKA_TOPIC = 'sp_ksql_views_per_page_30s') AS
    SELECT page,
           COUNT(*) AS views          -- WINDOWSTART / WINDOWEND are added automatically
    FROM pageviews
    WINDOW TUMBLING (SIZE 30 SECONDS, GRACE PERIOD 10 SECONDS)
    GROUP BY page
    EMIT CHANGES;

-- 6) Hopping window: 1 minute windows every 20 seconds
CREATE TABLE IF NOT EXISTS views_per_country_hopping AS
    SELECT country, COUNT(*) AS views
    FROM pageviews_enriched
    WINDOW HOPPING (SIZE 60 SECONDS, ADVANCE BY 20 SECONDS)
    GROUP BY country
    EMIT CHANGES;

-- 7) Non-windowed aggregate = materialized view you can query by key (pull query)
CREATE TABLE IF NOT EXISTS user_activity AS
    SELECT user_id,
           COUNT(*) AS views,
           SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchases,
           LATEST_BY_OFFSET(page) AS last_page
    FROM pageviews
    GROUP BY user_id
    EMIT CHANGES;

-- ---------------------------------------------------------------------------------------------
-- Try interactively:
--   PUSH query (never ends, Ctrl+C):  SELECT * FROM purchases EMIT CHANGES;
--   PULL query (point-in-time):        SELECT * FROM user_activity WHERE user_id = 'u1';
--   windowed pull query:               SELECT * FROM views_per_page_30s WHERE page = '/cart';
--   SHOW STREAMS; SHOW TABLES; SHOW QUERIES; DESCRIBE user_activity EXTENDED;
--   EXPLAIN <query id>;    -- shows the Kafka Streams topology behind the SQL
-- Clean up: DROP TABLE user_activity DELETE TOPIC; (etc.)
