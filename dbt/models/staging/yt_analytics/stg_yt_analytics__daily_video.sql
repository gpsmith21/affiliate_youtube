-- Before performing casting operations, deduplicate to include only the most recently refreshed rows for each unique video x day
WITH add_rn AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY video_id, "day"
            ORDER BY s3_run_ts DESC
        ) AS rn
    FROM {{ source('yt_analytics', 'daily_video') }}
),
deduped AS (
    SELECT *
    FROM add_rn
    WHERE rn = 1
)
-- Perform type casting and column renaming (snake_case) on deduped data
SELECT
    CAST("day" AS DATE) AS activity_date,
    video_id,
    CAST(views AS INTEGER) AS views,
    CAST(engagedViews AS INTEGER) AS engaged_views,
    CAST(estimatedMinutesWatched AS INTEGER) AS minutes_watched,
    CAST(estimatedRevenue AS NUMERIC(10,2)) AS ad_revenue_usd,
    CAST(likes AS INTEGER) AS likes,
    CAST(dislikes AS INTEGER) AS dislikes,
    CAST(shares AS INTEGER) AS shares,
    CAST(subscribersGained AS INTEGER) AS subscribers_gained,
    CAST(subscribersLost AS INTEGER) AS subscribers_lost,
    s3_run_ts AS last_refreshed_utc
FROM deduped