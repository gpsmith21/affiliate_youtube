"""
This script resets the database and re-initializes:
- All schemas (raw schema per source, development, and production)
- Ingestion-related tables (others are materialized with dbt later in the pipeline)

"""
import os
from pathlib import Path
import psycopg
from dotenv import load_dotenv

def main():
    # Load environment variables and build connection string for DB connection
    env_dir = Path(__file__).parent.parent
    load_dotenv(env_dir / '.env')

    host, port = os.environ.get("PGHOST"), os.environ.get("PGPORT")
    dbname, user, password = os.environ.get("PGDATABASE"), os.environ.get("PGUSER"), os.environ.get("PGPASSWORD")
    conn_str = f"host={host} port={port} dbname={dbname} user={user} password={password}"

    # Load DDL SQL (/sql) into memory
    sql_dir = Path(__file__).parent / 'sql'

    with open(os.path.join(sql_dir, 'reset_db.sql'), 'r') as f:
        query_reset_db = f.read()

    with open(os.path.join(sql_dir, 'init_schemas.sql'), 'r') as f:
        query_init_schemas = f.read()

    with open(os.path.join(sql_dir, 'init_src_amazon.sql'), 'r') as f:
        query_init_src_amazon = f.read()

    # Open Postgres connection and perform DDL.
    with psycopg.connect(conninfo=conn_str) as conn:
        conn.execute(query_reset_db)
        conn.execute(query_init_schemas)
        conn.execute(query_init_src_amazon)

        # Fetch schemas and tables that were added
        with conn.cursor() as cur:
            cur.execute("""
                        SELECT schema_name
                        FROM information_schema.schemata
                        """)
            schemas = [s[0] for s in cur.fetchall()]

            cur.execute("""
                        SELECT t.table_name
                        FROM information_schema.tables AS t
                        WHERE t.table_schema = 'raw_amazon'
                        """)
            tables_amz = [s[0] for s in cur.fetchall()]

            cur.execute("""
                        SELECT table_name
                        FROM information_schema.tables AS t
                        WHERE t.table_schema = 'raw_youtube'
                        """)
            tables_yt = [s[0] for s in cur.fetchall()]

    # Ensure DDL was executed successfully
    new_schemas = [s for s in schemas if s in ['raw_amazon', 'raw_youtube', 'dev', 'prod_marts']]
    print(f'Successfully created schemas: {new_schemas}')
    print(f'Successfully created raw_amazon tables: {tables_amz}')
    print(f'Successfully created raw_youtube tables: {tables_yt}')


if __name__ == '__main__':
    main()
