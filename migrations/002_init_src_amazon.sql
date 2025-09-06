-- Ensure build process is idempotent
DROP TABLE IF EXISTS raw_amazon.commissions;
DROP TABLE IF EXISTS raw_amazon.orders;
DROP TABLE IF EXISTS raw_amazon.daily_clicks;


-- Amazon commissions at an order-item level grain. Commissions are credited when shipped. Includes negative-valued commissions for returns
CREATE TABLE raw_amazon.commissions (
    etl_loaded_at timestamptz default current_timestamp,
    source_file text,
    category_name text,
    product_name text,
    product_asin text,
    seller text,
    associate_tracking_id text,
    date_shipped text,
    unit_price text,
    qty_shipped text,
    is_return text,
    revenue text
    commission text,
    device_type text
);

-- Amazon orders originating from the associate's affiliate links with order details
CREATE TABLE raw_amazon.orders (
    etl_loaded_at timestamptz default current_timestamp,
    source_file text,
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

-- Daily snapshot capturing metrics: total affiliate link clicks and items ordered by seller (Amazon vs. third-party)
CREATE TABLE raw_amazon.daily_clicks (
    etl_loaded_at timestamptz default current_timestamp,
    source_file text,
    snapshot_date text,
    daily_clicks text,
    qty_ordered_amz text,
    qty_ordered_other text,
    qty_ordered_all text,
    conversion_rate_pct text
);