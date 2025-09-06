-- Define a table in which our .csv data will land
CREATE TABLE IF NOT EXISTS raw_amazon.daily_clicks_landing (
    snapshot_date text,
    daily_clicks text,
    qty_ordered_amz text,
    qty_ordered_other text,
    qty_ordered_all text,
    conversion_rate_pct text
);

-- Copy raw .csv data into the landing table
-- \copy doesn't allow variable substitution by default, so we build the command by constructing the string and storing as a variable, and then execute it
\set copy_command '\\copy raw_amazon.daily_clicks_landing (snapshot_date, daily_clicks, qty_ordered_amz, qty_ordered_other, qty_ordered_all, conversion_rate_pct) FROM ' :'source_path' ' WITH (FORMAT csv, HEADER, DELIMITER '','', QUOTE E''\\b'', ENCODING ''UTF8'');'
:copy_command

BEGIN; -- All-at-once transaction

-- Delete stale rows in raw_amazon.daily_clicks table covering the same year but with an older refresh date
DELETE FROM raw_amazon.daily_clicks
    WHERE EXTRACT(YEAR FROM raw_amazon.daily_clicks.snapshot_date::date) = :year AND refresh_date < :'refresh_date';


-- What is the most recent refresh_date in our raw table for data covering the same year as our input .csv?
SELECT MAX(refresh_date) AS latest_refresh_date
FROM raw_amazon.daily_clicks
WHERE EXTRACT(YEAR FROM raw_amazon.daily_clicks.snapshot_date::date) = :year
\gset

-- If no refresh date was found, set to a default value
\if :{?latest_refresh_date}
  -- var exists, do nothing
\else
  \set latest_refresh_date '1900-01-01'
\endif

-- Insert our landing table contents into the raw daily_clicks table along with metadata regarding this insertion, IF it is the most up-to-date refresh.
INSERT INTO raw_amazon.daily_clicks
    (snapshot_date, daily_clicks, qty_ordered_amz, qty_ordered_other, qty_ordered_all, conversion_rate_pct,
    etl_loaded_at, refresh_date, source_csv)
SELECT *,
    current_timestamp,
    :'refresh_date'::date,
    :'source_path'::text
FROM raw_amazon.daily_clicks_landing
WHERE :'refresh_date' > :'latest_refresh_date';

TRUNCATE raw_amazon.daily_clicks_landing;

COMMIT; -- End transaction