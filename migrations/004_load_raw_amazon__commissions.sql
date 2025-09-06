-- quick debug: print what psql thinks source_path is
SHOW client_encoding;
SET client_encoding TO 'UTF8';

\echo source_path=[[:'source_path']]

-- Define a table in which our .csv data will land
CREATE TABLE IF NOT EXISTS raw_amazon.commissions_landing (
    category_name text,
    product_name text,
    product_asin text,
    seller text,
    associate_tracking_id text,
    date_shipped text,
    unit_price text,
    qty_shipped text,
    is_return text,
    revenue text,
    commission text,
    device_type text
);

-- Copy raw .csv data into the landing table
\copy raw_amazon.commissions_landing (category_name, product_name, product_asin, seller, associate_tracking_id, date_shipped, unit_price, qty_shipped, is_return, revenue, commission, device_type) FROM 'C:/Users/gpsmi/AE-DE/affiliate_youtube/raw_data/amazon/2023-Fee-Earnings-2025-09-02.csv' WITH (FORMAT csv, HEADER, DELIMITER ',', ENCODING 'UTF8');

BEGIN; -- All-at-once transaction

-- Remove any "stale" rows in raw_amazon.commissions containing data for the year covered by the .csv
DELETE FROM raw_amazon.commissions
    WHERE EXTRACT(YEAR FROM CAST(raw_amazon.commissions.date_shipped AS date)) = :year;

-- Insert our temporary table contents into the raw commissions table along with metadata regarding this insertion
INSERT INTO raw_amazon.commissions
    (category_name, product_name, product_asin, seller, associate_tracking_id, date_shipped, unit_price, qty_shipped, is_return, revenue, commission, device_type,
    etl_loaded_at, refresh_date, source_csv)
SELECT *, current_timestamp, :'refresh_date'::date, :'source_path'::text
FROM raw_amazon.commissions_landing;

TRUNCATE raw_amazon.commissions_landing;

COMMIT; -- End transaction