-- STREAM x CDC TABLE join: enrich purchase clicks with product data captured from MySQL by Debezium,
-- then aggregate revenue per category. The result is an UPDATING table -> upsert-kafka sink.
--
-- This is a *regular* join: Flink keeps both sides in state. If a product's price changes in MySQL,
-- Flink RETRACTS the old joined rows and re-emits them with the new price, so revenue is recomputed
-- (try it: 16-cdc-debezium-mysql/03_generate_changes.py changes prices).
-- Want "price at the time of purchase" instead? That's an event-time TEMPORAL join:
--     ... JOIN products FOR SYSTEM_TIME AS OF c.event_time AS p ON c.product_id = p.id
-- which needs a watermark on the products table as well (see README).
SET 'pipeline.name' = 'cdc-join-category-revenue';

INSERT INTO category_revenue
SELECT p.category,
       COUNT(*)                     AS purchases,
       ROUND(SUM(c.quantity * p.price), 2) AS revenue
FROM clicks AS c
JOIN products AS p ON c.product_id = p.id
WHERE c.action = 'purchase'
GROUP BY p.category;
