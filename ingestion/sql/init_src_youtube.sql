-- YouTube engagement metrics at a date x video grain
CREATE TABLE IF NOT EXISTS raw_youtube.daily_video (
    comments text,
    "day" text,
    dislikes text,
    engagedViews text,
    estimatedMinutesWatched text,
    estimatedRevenue text,
    likes text,
    shares text,
    subscribersGained text,
    subscribersLost text,
    video_id text,
    views text,
    s3_run_ts timestamptz,
    wh_loaded_at timestamptz DEFAULT current_timestamp
);

-- Subset of YouTube engagement metrics at a date x video x devicetype grain
CREATE TABLE IF NOT EXISTS raw_youtube.daily_video_devicetype (
    "day" text,
    deviceType text,
    engagedViews text,
    estimatedMinutesWatched text,
    video_id text,
    views text,
    s3_run_ts timestamptz,
    wh_loaded_at timestamptz DEFAULT current_timestamp
);