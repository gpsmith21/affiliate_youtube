# Import environment variables for database connection and csv pathing
set -a
source "$(dirname "$0")/.env"   # exports the above vars
set +a

# Common psql flags
PSQL="psql --no-psqlrc --set=ON_ERROR_STOP=1 --echo-queries --echo-errors --quiet"

# Work from a clean database. Drop raw schemas and re-initialize the raw_amazon schema's tables.
$PSQL -f "$(dirname "$0")/../amazon_psql/999_reset_raw_schemas.sql"
$PSQL -f "$(dirname "$0")/../amazon_psql/001_init_schemas.sql"
$PSQL -f "$(dirname "$0")/../amazon_psql/002_init_src_amazon.sql"

echo "Test 1:  Load commissions data for year 2023, effective 2025-09-02. Ensure this data is loaded."
file="${CSV_DIR}/2023-Fee-Earnings-2025-09-02.csv"
$PSQL -v year="2023" -v refresh_date="2025-09-02" -v source_path="$file" \
        -f "$(dirname "$0")/../amazon_psql/004_load_raw_amazon__commissions.sql"

$PSQL -v schema="raw_amazon" -v table="commissions" -v date_col="date_shipped" \
        -f "$(dirname "$0")/../amazon_psql/100_summary_stats_amazon.sql"


echo "Test 2: Load commissions data for year 2023, effective 2025-09-01. Ensure this new data was not loaded."
file="${CSV_DIR}/2023-Fee-Earnings-2025-09-01.csv"
$PSQL -v year="2023" -v refresh_date="2025-09-01" -v source_path="$file" \
        -f "$(dirname "$0")/../amazon_psql/004_load_raw_amazon__commissions.sql"

$PSQL -v schema="raw_amazon" -v table="commissions" -v date_col="date_shipped" \
        -f "$(dirname "$0")/../amazon_psql/100_summary_stats_amazon.sql"
        

echo "Test 3: Load commissions data for year 2023, effective 2025-09-03. Ensure old data was overwritten."
file="${CSV_DIR}/2023-Fee-Earnings-2025-09-03.csv"
$PSQL -v year="2023" -v refresh_date="2025-09-03" -v source_path="$file" \
        -f "$(dirname "$0")/../amazon_psql/004_load_raw_amazon__commissions.sql"

$PSQL -v schema="raw_amazon" -v table="commissions" -v date_col="date_shipped" \
        -f "$(dirname "$0")/../amazon_psql/100_summary_stats_amazon.sql"


echo "Test 4: Load commissions data for year 2025, effective 2025-09-02. Ensure this load succeeds and does not affect the 2023 data."
file="${CSV_DIR}/2025-Fee-Earnings-2025-09-02.csv"
$PSQL -v year="2025" -v refresh_date="2025-09-02" -v source_path="$file" \
        -f "$(dirname "$0")/../amazon_psql/004_load_raw_amazon__commissions.sql"

$PSQL -v schema="raw_amazon" -v table="commissions" -v date_col="date_shipped" \
        -f "$(dirname "$0")/../amazon_psql/100_summary_stats_amazon.sql"


echo "Test 5: Load commissions data for year 2024, effective 2025-09-04. Ensure this load succeeds and does not affect 2023 or 2025 data."
file="${CSV_DIR}/2024-Fee-Earnings-2025-09-04.csv"
$PSQL -v year="2024" -v refresh_date="2025-09-04" -v source_path="$file" \
        -f "$(dirname "$0")/../amazon_psql/004_load_raw_amazon__commissions.sql"

$PSQL -v schema="raw_amazon" -v table="commissions" -v date_col="date_shipped" \
        -f "$(dirname "$0")/../amazon_psql/100_summary_stats_amazon.sql"


# Reset raw_amazon schema.
$PSQL -f "$(dirname "$0")/../amazon_psql/999_reset_raw_schemas.sql"

echo "Tests completed."