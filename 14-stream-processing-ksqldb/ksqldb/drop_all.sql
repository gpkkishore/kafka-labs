-- Remove everything queries.sql created:  ksql> RUN SCRIPT '/ksql/drop_all.sql';
-- DROP ... DELETE TOPIC also terminates the persistent query writing to it. Stopping a query takes a few
-- seconds; if the CLI reports "Timeout while waiting for command topic consumer", just run the script again.
DROP TABLE IF EXISTS user_activity DELETE TOPIC;
DROP TABLE IF EXISTS views_per_country_hopping DELETE TOPIC;
DROP TABLE IF EXISTS views_per_page_30s DELETE TOPIC;
DROP STREAM IF EXISTS pageviews_enriched DELETE TOPIC;
DROP STREAM IF EXISTS purchases DELETE TOPIC;
DROP TABLE IF EXISTS users;
DROP STREAM IF EXISTS pageviews;
