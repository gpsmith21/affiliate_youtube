"""
Idempotent load of YouTube Analytics API pulls from s3 into our "warehouse" (append-only).
- Search the source=youtubeanalytics_v2 namespace in our s3 bucket for .parquet files.
- Query warehouse tables in raw_youtube to prune files which have already been loaded.
- For those which have not been loaded, server-side COPY into our db with metadata columns.
"""
import json
from pathlib import Path
import os
import pandas as pd
import boto3
import psycopg
from psycopg import sql
from io import BytesIO
import datetime as dt
from dotenv import load_dotenv


# Given a source and report name, retrieve all load timestamps in our S3 bucket associated with this report
def retrieve_report_timestamps(bucket_name, schema_name, source_name, report_name):
    s3 = boto3.client('s3')
    c_token = None
    obj_list = []

    # Implement API pagination
    while True:
        kwargs = {'Bucket': bucket_name,
                  'Prefix': f'{schema_name}/source={source_name}/report={report_name}'}
        if c_token:
            kwargs['ContinuationToken'] = c_token

        response = s3.list_objects_v2(**kwargs)
        
        # Record all valid object keys and their corresponding run timestamps
        for entry in response['Contents']:
            key_list = entry['Key'].split('/')
            for key in key_list:
                if key.startswith('run_ts='):
                    obj_list.append({'run_ts': key.lstrip('run_ts='),
                                    's3_key': entry['Key']
                                    })
                    
        if not response.get('IsTruncated'):
            break
        c_token = response.get('NextContinuationToken')
         
    return obj_list


if __name__ == '__main__':

    # Constants
    ROOT = Path(__file__).parent.parent
    BUCKET = 'affiliate-youtube-project'
    S3_SCHEMA = 'raw'
    load_dotenv(ROOT / '.env')

    # Load current s3 schema which defines mapping between s3 partitions and warehouse tables
    s3_schema_path = ROOT / 'ingestion' / 's3_schema.json'
    with open(s3_schema_path, 'r') as f:
        schema = json.load(f)
    print(schema)
    
    s3 = boto3.client('s3')
    now = dt.datetime.now(dt.timezone.utc)
    
    # Source has 1:many relationship with reports. Each report maps to a target table in the corresponding source's schema.
    for source_obj in schema['sources']:
        # Not currently implemented for amazon
        if source_obj['source_name'] != 'youtubeanalytics_v2':
            continue

        for report_obj in source_obj['reports']:
            
            # All runs (timestamp and s3 key) for current source, report
            all_runs = retrieve_report_timestamps(BUCKET, S3_SCHEMA, source_obj['source_name'], report_obj['report_name'])

            # Scan corresponding warehouse table for existing timestamps
            host, port = os.environ.get("PGHOST"), os.environ.get("PGPORT")
            dbname, user, password = os.environ.get("PGDATABASE"), os.environ.get("PGUSER"), os.environ.get("PGPASSWORD")
            conn_str = f"host={host} port={port} dbname={dbname} user={user} password={password}"

            with psycopg.connect(conn_str) as conn:
                # Scan relevant warehouse table for files (identified by s3_run_ts timestamp) already present for this table
                with conn.cursor() as cur:
                        cur.execute(sql.SQL("SELECT DISTINCT s3_run_ts FROM {}")
                                    .format(sql.Identifier(source_obj['target_wh_schema'], report_obj['target_wh_table'])))
                        found_ts = [val[0].strftime("%Y%m%d%H%M%S") for val in cur.fetchall()]
                        print(found_ts)
                print(f'Found the following timestamps for report {report_obj}: {found_ts}')
                
                # Perform server-side copy for runs not yet loaded into the target warehouse table
                # One transaction per file; on error, skip, print error message, and continue
                for run in all_runs:
                    if run['run_ts'] in found_ts:
                        continue
                    try: 
                        with conn.transaction():
                            with conn.cursor() as cur:
                                # Download the s3 object associated with this key
                                buf = BytesIO()
                                parquet_file = s3.download_fileobj(BUCKET, run['s3_key'], buf)
                                buf.seek(0)

                                # Add metadata columns and convert df to iterable of tuples
                                df = pd.read_parquet(buf)
                                df['s3_run_ts'] = dt.datetime.strptime(run['run_ts'], "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc)
                                df['wh_loaded_at'] = now
                                df_fmt = list(df.itertuples(index=False, name=None))

                                # Enforce columns are as expected (s3_schema.json)
                                for col in df.columns:
                                    if col not in report_obj['cols']:
                                        if col not in ['s3_run_ts', 'wh_loaded_at']:
                                            raise ValueError(f'Read unexpected column {col} from file {run}. Aborting copy operation.')
                                        
                                # Perform server-side copy. Specify column ordering for safety      
                                with cur.copy(sql.SQL('COPY {} ({}) FROM STDIN').format(
                                    sql.Identifier(source_obj['target_wh_schema'], report_obj['target_wh_table']),
                                    sql.SQL(','.join(df.columns)))) as copy:
                                    for record in df_fmt:
                                        copy.write_row(record)

                                print(f'Successfully copied report:\n {report_obj}\n From S3 file:\n{run}')


                    except Exception as e:
                        print(f'Encountered error while copying.\nReport: {report_obj}\n File: {run}\n Skipping this file.')
                        print(f'Error details: {e}')