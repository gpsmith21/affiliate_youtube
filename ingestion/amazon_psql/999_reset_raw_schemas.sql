-- Drop raw_amazon_psql schema; run seed_db.sh to repopulate
DROP TABLE IF EXISTS raw_amazon_psql.commissions;
DROP TABLE IF EXISTS raw_amazon_psql.orders;
DROP TABLE IF EXISTS raw_amazon_psql.daily_clicks;
DROP SCHEMA raw_amazon_psql CASCADE;