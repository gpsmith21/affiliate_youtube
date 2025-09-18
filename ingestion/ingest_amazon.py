"""
Moves all .csv files from root/raw_data/amazon to the raw_amazon schema in append-only fashion.
- Discover all files and parse csv names to feed metadata ingestion columns
- Clean .csv files received from data producer for COPY
- Copy each .csv file into the appropriate raw_amazon table with a buffered approach

"""

from pathlib import Path
import os
import datetime as dt
from dotenv import load_dotenv
import psycopg
from psycopg import sql
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
            print(f'ERROR: Non-csv file found in raw_data dir: {path_obj.name}. Skipping this file.')
            continue
        
        # Ensure csv filename is formatted correctly
        # Expected args ('-' separated):
        #   - YYYY (relevant year for csv data)
        #   - report prefix, suffix
        #   - ISO formatted refresh date (YYYY-MM-DD),
        try:
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
        csv_metadata.append(d)

    return csv_metadata


# Read amazon csv files into memory and COPY
def ingest_amazon_csv_files(csv_metadata):

    # Load environment variables and build connection string for DB connection
    env_dir = Path(__file__).parent.parent
    load_dotenv(env_dir / '.env')

    host, port = os.environ.get("PGHOST"), os.environ.get("PGPORT")
    dbname, user, password = os.environ.get("PGDATABASE"), os.environ.get("PGUSER"), os.environ.get("PGPASSWORD")
    conn_str = f"host={host} port={port} dbname={dbname} user={user} password={password}"

    with psycopg.connect(conn_str) as conn:

        # Keep track of all previously loaded files to avoid double-loading
        with conn.cursor() as cur:
            loaded_files = {}
            for table_name in ['commissions', 'orders', 'daily_clicks']:
                cur.execute(sql.SQL("SELECT DISTINCT source_csv FROM {}").format(sql.Identifier('raw_amazon', table_name)))
                loaded_files[table_name] = [val[0] for val in cur.fetchall()]
            print(loaded_files)

            batch_id = 1
            etl_loaded_at = dt.date.today()
        
        # For each file, load into the database if it hasn't yet been loaded.
        for curr_csv in csv_metadata:
            target_table = curr_csv['target_table']
            curr_csv_name = curr_csv['file_path'].name 
            if curr_csv_name in loaded_files[target_table]:
                print(f'INFO: File {curr_csv_name} already ingested. Skipping this file.')
                continue

            # Try to copy each file in its own transaction; if it fails, rollback this file only and continue
            try:
                with conn.transaction():
                    loaded_files[target_table].append(curr_csv['file_path'].name)
                    with conn.cursor() as cur:
                
                        with open(curr_csv['file_path'], 'r', newline='', encoding='utf8') as f:
                            # Align dialect to data producer's (Amazon's) source formatting
                            reader = csv.reader(f, delimiter=',', quotechar='\b')

                            with cur.copy(sql.SQL("COPY {} FROM STDIN")
                                          .format(sql.Identifier('raw_amazon', target_table))) as copy:
                                
                                for row in reader:
                                    # First 2 rows are metadata, then headers
                                    if reader.line_num <= 2:
                                        continue

                                    # Add batch-level metadata columns, then copy into our target table
                                    row.append(batch_id)
                                    row.append(etl_loaded_at)
                                    row.append(curr_csv['file_path'].name)
                                    row.append(curr_csv['refresh_date'])

                                    # Copy row into target table
                                    copy.write_row(row)

            except Exception as e:
                loaded_files[target_table].pop()
                print(e)
                print(f'Encountered error while copying {curr_csv_name}. Skipping this file.')


if __name__ == '__main__':
    csv_metadata = parse_csv_landing_dir()
    ingest_amazon_csv_files(csv_metadata)