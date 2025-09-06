-- Define a table in which our .csv data will land
CREATE TABLE IF NOT EXISTS raw_amazon.orders_landing (
    category_name text,
    product_name text,
    product_asin text,
    order_date text,
    qty_ordered text,
    unit_price text,
    link_type text,
    associate_tracking_id text,
    indirect_sale text,
    device_type text
);

-- Copy raw .csv data into the landing table
-- \copy doesn't allow variable substitution by default, so we build the command by constructing the string and storing as a variable, and then execute it
\set copy_command '\\copy raw_amazon.orders_landing (category_name, product_name, product_asin, order_date, qty_ordered, unit_price, link_type, associate_tracking_id, indirect_sale, device_type) FROM ' :'source_path' ' WITH (FORMAT csv, HEADER, DELIMITER '','', QUOTE E''\\b'', ENCODING ''UTF8'');'
:copy_command

BEGIN; -- All-at-once transaction

-- Delete stale rows in raw_amazon.orders table covering the same year but with an older refresh date
DELETE FROM raw_amazon.orders
    WHERE EXTRACT(YEAR FROM raw_amazon.orders.order_date::date) = :year AND refresh_date < :'refresh_date';


-- What is the most recent refresh_date in our raw table for data covering the same year as our input .csv?
SELECT MAX(refresh_date) AS latest_refresh_date
FROM raw_amazon.orders
WHERE EXTRACT(YEAR FROM raw_amazon.orders.order_date::date) = :year
\gset

-- If no refresh date was found, set to a default value
\if :{?latest_refresh_date}
  -- var exists, do nothing
\else
  \set latest_refresh_date '1900-01-01'
\endif

-- Insert our landing table contents into the raw orders table along with metadata regarding this insertion, IF it is the most up-to-date refresh.
INSERT INTO raw_amazon.orders
    (category_name, product_name, product_asin, order_date, qty_ordered, unit_price, link_type, associate_tracking_id, indirect_sale, device_type,
    etl_loaded_at, refresh_date, source_csv)
SELECT *,
    current_timestamp,
    :'refresh_date'::date,
    :'source_path'::text
FROM raw_amazon.orders_landing
WHERE :'refresh_date' > :'latest_refresh_date';

TRUNCATE raw_amazon.orders_landing;

COMMIT; -- End transaction