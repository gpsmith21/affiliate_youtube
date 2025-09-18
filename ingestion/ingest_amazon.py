"""
Moves all .csv files from root/raw_data/amazon to the raw_amazon schema in append-only fashion.
- Discover all files and parse csv names to supply batch medata columns
- Clean .csv files received from data producer for COPY
- Copy each .csv file into the appropriate raw_amazon table with a buffered approach

"""

from pathlib import Path
import os
import datetime as dt
from dotenv import load_dotenv
import csv

# Read contents of raw_data/amazon directory and return per-csv metadata for ingestion step.
# Skip files which do not conform to the expected format and naming convention with descriptive error messages, rather than halting execution.
def parse_csv_landing_dir():

    # Mapping of amazon report names to my database table names
    amz_report_map = {'Fee-Orders': 'orders', 'Fee-Earnings': 'commissions', 'Fee-DailyTrends': 'daily_clicks'}

    raw_data_path = Path(__file__).parent.parent / 'raw_data' / 'amazon'
    csv_metadata = []

    for path_obj in raw_data_path.iterdir():

        # Only process CSVs
        if path_obj.suffix != '.csv':
            print(path_obj.suffix)
            print(f'WARNING: Non-csv file found in raw_data dir: {path_obj.name}')
            continue
        
        # Ensure csv filename is formatted correctly
        # Expected: YYYY (relevant year for csv data), report prefix and suffix, ISO formatted refresh date (YYYY-MM-DD), "-" separated
        try:
            print(path_obj.stem)
            csv_stem_split = tuple(path_obj.stem.split('-'))
            data_year, report_prefix, report_suffix, refresh_y, refresh_m, refresh_d = csv_stem_split
        except ValueError as e:
            print(f'ERROR: {path_obj.name} contains {len(csv_stem_split)} args, expected 6: correct format is YYYY-prefix-suffix-YYYY-MM-DD.csv. Skipping this file.')
            continue

        # Ensure all parts of csv string referring to time periods are parseable as integers
        try:
            data_year, refresh_y, refresh_m, refresh_d = int(data_year), int(refresh_y), int(refresh_m), int(refresh_d)
        except ValueError as e:
            print(f'ERROR: Could not parse date values in {path_obj.name}: correct format is YYYY-prefix-suffix-YYYY-MM-DD.csv. Skipping this file.')
            continue

        # Ensure report_prefix + report_suffix represents a supported Amazon report
        try:
            amazon_report_name = f'{report_prefix}-{report_suffix}'
            target_table = amz_report_map[amazon_report_name]
        except KeyError as e:
            print(f'ERROR: {path_obj.name} includes unsupported Amazon report {amazon_report_name}. Valid values include {[val for val in amz_report_map.values()]}')
            continue
        
        # Create a dictionary capturing this dataset's parsed metadata
        d = {
            'file_path': path_obj,
            'data_year': data_year,
            'refresh_date': dt.date(refresh_y, refresh_m, refresh_d),
            'target_table': target_table
        }
        csvs_metadata.append(d)

    return csvs_metadata


# 
def ingest_csv_data_amazon(csvs_metadata,)

if __name__ == '__main__':
    csvs_metadata = parse_csv_landing_dir()
    #print(csvs_metadata)