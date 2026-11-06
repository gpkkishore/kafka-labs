-- Paste these into the interactive shell:
--   docker compose -f 17-flink-sql/docker-compose.yml run --rm sql-client
-- (tables from 00_init_tables.sql are already defined there)

SET 'sql-client.execution.result-mode' = 'tableau';   -- print rows instead of the full-screen viewer

SHOW TABLES;
DESCRIBE clicks;

-- Peek at raw events incl. Kafka metadata and the computed event_time
SELECT kafka_partition, user_id, product_id, action, event_time FROM clicks LIMIT 10;

-- The CDC table as Flink sees it right now (a changelog materialized into a table)
SELECT * FROM products;

-- Watch a changelog: run this, then change a price in MySQL -> -U / +U rows appear
SET 'sql-client.execution.result-mode' = 'changelog';
SELECT category, COUNT(*) AS n, MAX(price) AS max_price FROM products GROUP BY category;

-- Session window: group a user's clicks separated by gaps of > 20s into "sessions"
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT window_start, window_end, user_id, COUNT(*) AS clicks
FROM TABLE(SESSION(TABLE clicks PARTITION BY user_id, DESCRIPTOR(event_time), INTERVAL '20' SECOND))
GROUP BY window_start, window_end, user_id;

-- Top-N per window: 2 most viewed products per 30s window
SELECT * FROM (
  SELECT window_start, product_id, view_count,
         ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY view_count DESC) AS rn
  FROM (SELECT window_start, window_end, product_id, COUNT(*) AS view_count
        FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(event_time), INTERVAL '30' SECOND))
        WHERE action = 'view' GROUP BY window_start, window_end, product_id))
WHERE rn <= 2;
