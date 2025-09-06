-- Ensure build process is idempotent
DROP TABLE IF EXISTS raw_amazon.commissions;
DROP TABLE IF EXISTS raw_amazon.orders;
DROP TABLE IF EXISTS raw_amazon.daily_clicks;

DROP SCHEMA raw_amazon CASCADE;
DROP SCHEMA raw_youtube CASCADE;