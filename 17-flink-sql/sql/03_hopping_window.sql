-- HOPPING (sliding) window: 30s long, a new window every 10s -> each event counts in 3 windows.
-- "Trending products": views per product over the last 30s, refreshed every 10s.
SET 'pipeline.name' = 'hopping-trending-products';

INSERT INTO trending_products
SELECT window_start, window_end, product_id, COUNT(*) AS view_count
FROM TABLE(HOP(TABLE clicks, DESCRIPTOR(event_time), INTERVAL '10' SECOND, INTERVAL '30' SECOND))
WHERE action = 'view'
GROUP BY window_start, window_end, product_id;
