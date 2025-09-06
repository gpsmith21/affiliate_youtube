-- Drop raw_amazon and raw_youtube schemas; run seed_db.sh to repopulate
DROP TABLE IF EXISTS raw_amazon.commissions;
DROP TABLE IF EXISTS raw_amazon.orders;
DROP TABLE IF EXISTS raw_amazon.daily_clicks;

DROP SCHEMA raw_amazon CASCADE;
DROP SCHEMA raw_youtube CASCADE;