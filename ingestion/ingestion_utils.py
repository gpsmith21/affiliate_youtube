"""
Utility functions applicable to several scripts in the ingestion step for this project.
"""

import datetime as dt

# For consistent key structure across sources
def generate_s3_key(run_timestamp, source, api_version, desc, format):

    if isinstance(api_version, str):
        source = f'{source}_{api_version}'
    elif api_version is not None:
        print(f'Warning: API version argument is not a string and will not be included: {api_version}')
    
    run_timestamp_str = run_timestamp.strftime("%Y%m%d%H%M%S")

    filename = f'{desc}_Run_{run_timestamp_str}{format}'

    return f'raw/source={source}/report={desc}/run_ts={run_timestamp_str}/data.parquet'