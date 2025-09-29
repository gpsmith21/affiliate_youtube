-- YouTube engagement metrics at a date x video grain
CREATE TABLE IF NOT EXISTS raw_youtube.daily_video (
    comments text,
    snapshot_date text,
    dislikes text,
    engaged_views text,
    minutes_watched text,
    revenue text,
    likes text,
    shares text,
    subscribers_gained text,
    subscribers_lost text,
    video_id text,
    views text,
    s3_run_ts timestamptz,
    wh_loaded_at timestamptz DEFAULT current_timestamp
);

-- Subset of YouTube engagement metrics at a date x video x devicetype grain
CREATE TABLE IF NOT EXISTS raw_youtube.daily_video_devicetype (
    snapshot_date text,
    device_type text,
    engaged_views text,
    minutes_watched text,
    video_id text,
    views text,
    s3_run_ts timestamptz,
    wh_loaded_at timestamptz DEFAULT current_timestamp
);