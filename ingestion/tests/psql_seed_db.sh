# Import environment variables for database connection and csv pathing
set -a
source "$(dirname "$0")/.env"   # exports the above vars
set +a

# Common psql flags
PSQL="psql --no-psqlrc --set=ON_ERROR_STOP=1 --echo-queries --echo-errors --quiet"

# Create database schemas and raw data tables if they don't exist
$PSQL -f "$(dirname "$0")/../amazon_psql/001_init_schemas.sql"
$PSQL -f "$(dirname "$0")/../amazon_psql/002_init_src_amazon.sql"
$PSQL -f "$(dirname "$0")/../amazon_psql/003_init_land_amazon.sql"


# Before loading, print summary stats about each table
echo "State of raw_amazon.commissions prior to seeding: "
$PSQL -v schema="raw_amazon" -v table="commissions" -v date_col="date_shipped" \
        -f "$(dirname "$0")/../amazon_psql/100_summary_stats_amazon.sql"

echo "State of raw_amazon.orders prior to seeding: "
$PSQL -v schema="raw_amazon" -v table="orders" -v date_col="order_date" \
        -f "$(dirname "$0")/../amazon_psql/100_summary_stats_amazon.sql"

echo "State of raw_amazon.daily_clicks prior to seeding: "
$PSQL -v schema="raw_amazon" -v table="daily_clicks" -v date_col="snapshot_date" \
        -f "$(dirname "$0")/../amazon_psql/100_summary_stats_amazon.sql"


# Seed Amazon commissions table
echo "Seeding Amazon commissions table: "
for year in 2023 2024 2025; do
    file="${CSV_DIR}/${year}-Fee-Earnings-${REFRESH_DATE}.csv"
    $PSQL -v year="$year" -v refresh_date="${REFRESH_DATE}" -v source_path="$file" \
            -f "$(dirname "$0")/../amazon_psql/004_load_raw_amazon__commissions.sql"
done

# Seed Amazon orders table
echo "Seeding Amazon orders table: "
for year in 2023 2024 2025; do
    file="${CSV_DIR}/${year}-Fee-Orders-${REFRESH_DATE}.csv"
    $PSQL -v year="$year" -v refresh_date="${REFRESH_DATE}" -v source_path="$file" \
            -f "$(dirname "$0")/../amazon_psql/005_load_raw_amazon__orders.sql"
done

# Seed Amazon daily_clicks table
echo "Seeding Amazon daily_clicks table: "
for year in 2023 2024 2025; do
    file="${CSV_DIR}/${year}-Fee-DailyTrends-${REFRESH_DATE}.csv"
    $PSQL -v year="$year" -v refresh_date="${REFRESH_DATE}" -v source_path="$file" \
            -f "$(dirname "$0")/../amazon_psql/006_load_raw_amazon__daily_clicks.sql"
done

# After loading, print summary stats about each table
echo "State of raw_amazon.commissions after seeding: "
$PSQL -v schema="raw_amazon" -v table="commissions" -v date_col="date_shipped" \
        -f "$(dirname "$0")/../amazon_psql/100_summary_stats_amazon.sql"

echo "State of raw_amazon.orders after seeding: "
$PSQL -v schema="raw_amazon" -v table="orders" -v date_col="order_date" \
        -f "$(dirname "$0")/../amazon_psql/100_summary_stats_amazon.sql"

echo "State of raw_amazon.daily_clicks after seeding: "
$PSQL -v schema="raw_amazon" -v table="daily_clicks" -v date_col="snapshot_date" \
        -f "$(dirname "$0")/../amazon_psql/100_summary_stats_amazon.sql"

echo "Finished seeding tables."