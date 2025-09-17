\echo 'Number of rows:'
SELECT
    COUNT(*) AS num_rows
FROM :schema.:table;

\echo 'Latest refresh date by event year:'
WITH grouped_by_year AS (
    SELECT
        refresh_date,
        EXTRACT(year FROM :date_col::date) AS event_year
    FROM :schema.:table
)
SELECT
    event_year,
    MAX(refresh_date) AS latest_refresh_date
FROM grouped_by_year
GROUP BY event_year
ORDER BY event_year;
