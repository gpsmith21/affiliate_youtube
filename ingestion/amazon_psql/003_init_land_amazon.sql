-- Define 3 tables in which our .csv data will land.
-- Only difference vs. tables in step 002 is no metadata columns (etl_loaded_at, refresh_date, or source .csv path)

CREATE TABLE IF NOT EXISTS raw_amazon_psql.commissions_landing (
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

CREATE TABLE IF NOT EXISTS raw_amazon_psql.orders_landing (
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

CREATE TABLE IF NOT EXISTS raw_amazon_psql.daily_clicks_landing (
    snapshot_date text,
    daily_clicks text,
    qty_ordered_amz text,
    qty_ordered_other text,
    qty_ordered_all text,
    conversion_rate_pct text
);
