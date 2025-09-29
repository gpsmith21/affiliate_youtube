"""
Idempotent load of YouTube Analytics API pulls from s3 into our "warehouse" (append-only).
- Search the source=youtubeanalytics_v2 namespace in our s3 bucket for .parquet files.
- Query warehouse tables in raw_youtube to prune files which have already been loaded.
- For those which have not been loaded, server-side COPY into our db with metadata columns.
"""
import json
from pathlib import Path
import pandas as pd
import boto3


# Given a source and report name, retrieve all load timestamps in our S3 bucket associated with this report
def retrieve_report_timestamps(bucket_name, schema_name, source_name, report_name):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(
        Bucket={bucket_name}
        Prefix=f'{schema_name}/source={source_name}/report={report_name}'
    )
    # # For debugging
    # with open('example_response.json', 'w') as f:
    #     json.dump(response, f, indent=2, default=str)
    
    ts_list = []
    for entry in response['Contents']:
        key_list = entry['Key'].split('/')
        for key in key_list:
            if key[:7] == 'run_ts=':
                ts_list.append({'run_ts': key[7:],
                                'prefix': entry['Key']
                                })
    return ts_list


# Given a list of timestamps and a target table, return the timestamps not already present
def find_unloaded_runs(ts_list, target_schema, target_table, target_col='etl_loaded_at'):
    # Get list of unique timestamps
    # Compare to runs we found
    pass


if __name__ == '__main__':
    # Load current s3 schema which defines mapping between s3 partitions and warehouse tables
    pass