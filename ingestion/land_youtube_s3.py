"""
Land youtube channel performance data received via the YouTube Analytics API in S3.
- Make GET requests to YouTube's API (daily granularity filtered to each video on the channel)
- Append daily totals for each video into one dataset using pandas
- Store as .parquet file in S3 so we can idempotently load into our warehouse
"""

from pathlib import Path
from functools import wraps
import time
import io
import datetime as dt
import pandas as pd
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import boto3

ROOT = Path(__file__).parent.parent
CREDS_PATH = ROOT / 'auth' / 'authorized_user.json'
NUM_RETRIES = 3
SLEEP_TIME = 2
load_dotenv(ROOT / '.env')

# Decorator for API request retries
def api_retry_decorator(func):
    @wraps(func)
    def wrapper(args, **kwargs):
        for i in range(1, NUM_RETRIES + 1):
            try:
                result = func(args, **kwargs)
                break
            except Exception as e:
                if i < NUM_RETRIES:
                    print(f'ERROR: {func.__name__} failed with arguments {args}, {kwargs} on attempt {i}. Waiting {SLEEP_TIME * i} seconds before retry. \nError: {e}')
                    time.sleep(SLEEP_TIME * i)
                else:
                    print(f'ERROR: All attempts to run {func.__name__} have failed.')
                    raise
        return result
    return wrapper


# Make an API request to the YouTube API for a daily view of various performance metrics for a specific video. with retry logic
@api_retry_decorator
def make_timebased_yt_request(video_id, start_date, end_date):
    result = yt_analytics.reports().query(
    ids='channel==MINE',
    startDate=start_date,
    endDate=end_date,
    metrics='engagedViews,views,comments,likes,dislikes,shares,estimatedMinutesWatched,subscribersGained,subscribersLost,estimatedRevenue',
    dimensions='day',
    sort='day',
    filters=f'video=={video_id}'
    ).execute()
    return result


# Make an API request to the YouTube API for a daily view of viewership metrics by viewer's device type
@api_retry_decorator
def make_devicetype_yt_request(video_id, start_date, end_date):
    result = yt_analytics.reports().query(
    ids='channel==MINE',
    startDate=start_date,
    endDate=end_date,
    metrics='engagedViews,views,estimatedMinutesWatched',
    dimensions='day,deviceType',
    sort='day',
    filters=f'video=={video_id}'
    ).execute()
    return result


# Loop through video IDs to request daily performance by video, then aggregate and return a dataframe object representing total performance by video and date.
def request_and_aggregate_report(video_ids, get_report_func, start_date='2023-08-23', end_date=dt.date.today()):

    # Convert dates from datetime/date to string if needed
    if isinstance(start_date, dt.datetime) or isinstance(start_date, dt.date):
        start_date = start_date.strftime("%Y-%m-%d")
    if isinstance(end_date, dt.datetime) or isinstance(end_date, dt.date):
        end_date = end_date.strftime("%Y-%m-%d")

    # Make API requests, collect results as list of dfs
    results = []
    for video_id in video_ids:
        result = get_report_func(video_id, start_date, end_date)

        col_names = []
        for entry in result['columnHeaders']:
            col_names.append(entry['name'])
        
        df = pd.DataFrame(result['rows'], columns=col_names)
        df['video_id'] = video_id
        df = df[sorted(df.columns)]
        results.append(df)

    # Join DFs together
    df_combined = pd.concat(results, ignore_index=True)
    return df_combined


if __name__ == '__main__':
    
    # Load creds from file
    creds = Credentials.from_authorized_user_file(CREDS_PATH, scopes=["https://www.googleapis.com/auth/yt-analytics.readonly",
              "https://www.googleapis.com/auth/yt-analytics-monetary.readonly"])

    # Create a YouTube Analytics API client object
    yt_analytics = build('youtubeanalytics', 'v2', credentials=creds)

    # Load unique YouTube IDs from seed file
    video_metadata = pd.read_csv(ROOT / 'raw_data' / 'YT_Videos_Bridge.csv')
    video_ids = list(video_metadata['youtube_id'].unique())

    # Make API requests for both types of reports
    df_timebased = request_and_aggregate_report(video_ids, make_timebased_yt_request)
    df_devicetype = request_and_aggregate_report(video_ids, make_devicetype_yt_request)

    # Write resulting dfs to in-memory parquet files
    buf_timebased, buf_devicetype = io.BytesIO(), io.BytesIO()
    df_timebased.to_parquet(buf_timebased, index=False)
    df_devicetype.to_parquet(buf_devicetype, index=False)
    buf_timebased.seek(0), buf_devicetype.seek(0)

    # Write these buffered IO objects to storage
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        print(bucket.name)