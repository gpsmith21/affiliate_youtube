-- Amazon commissions at an order-item level grain. Commissions are credited when shipped. Includes negative-valued commissions for returns
CREATE TABLE IF NOT EXISTS raw_amazon.commissions (
    "Category" text,
    "Name" text,
    "ASIN" text,
    "Seller" text,
    "Tracking ID" text,
    "Date Shipped" text,
    "Price($)" text,
    "Items Shipped" text,
    "Returns" text,
    "Revenue($)" text,
    "Ad Fees($)" text,
    "Device Type Group" text,
    wh_loaded_at timestamptz DEFAULT current_timestamp,
    source_csv text,
    refresh_date date
);

-- Amazon orders originating from the associate's affiliate links with order details
CREATE TABLE IF NOT EXISTS raw_amazon.orders (
    "Category" text,
    "Name" text,
    "ASIN" text,
    "Date" text,
    "Qty" text,
    "Price($)" text,
    "Link Type" text,
    "Tag" text,
    "Indirect Sales" text,
    "Device Type Group" text,
    wh_loaded_at timestamptz DEFAULT current_timestamp,
    source_csv text,
    refresh_date date
);

-- Daily snapshot capturing metrics: total affiliate link clicks and items ordered by seller (Amazon vs. third-party)
CREATE TABLE IF NOT EXISTS raw_amazon.daily_clicks (
    "Date" text,
    "Clicks" text,
    "Items Ordered (Amazon)" text,
    "Items Ordered (3rd Party)" text,
    "Total Items Ordered" text,
    "Conversion" text,
    wh_loaded_at timestamptz DEFAULT current_timestamp,
    source_csv text,
    refresh_date date
);