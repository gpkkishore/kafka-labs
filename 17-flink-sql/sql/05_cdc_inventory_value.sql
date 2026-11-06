-- Continuous aggregation directly over the CDC changelog: live stock value per category.
-- Every INSERT/UPDATE/DELETE in MySQL.products updates exactly one key in the output topic.
SET 'pipeline.name' = 'cdc-inventory-value';

INSERT INTO inventory_value
SELECT category,
       COUNT(*)                         AS product_count,
       CAST(SUM(stock) AS BIGINT)       AS units_in_stock,
       ROUND(SUM(stock * price), 2)     AS stock_value
FROM products
GROUP BY category;
