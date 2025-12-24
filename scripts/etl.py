#!/usr/bin/env python3
"""
etl.py

Simple, repeatable ETL to load CSVs from ./data into a Postgres DB.
- Reads CSVs: users.csv, products.csv, orders.csv, order_items.csv, reviews.csv
- Ensures schema 'ecom' exists (can be changed via ECOM_SCHEMA env var)
- Uses SQLAlchemy + pandas.to_sql (with chunksize and method='multi')
- Expects DATABASE_URL env var like:
    postgresql://postgres:password@host:5432/postgres?sslmode=require
"""

import os
import sys
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# Config
DB_URL = os.getenv("DATABASE_URL")
ECOM_SCHEMA = os.getenv("ECOM_SCHEMA", "ecom")
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

CSV_MAP = {
    "users": "users.csv",
    "products": "products.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "product_reviews": "reviews.csv",  # CSV name -> table name
}

# Safety checks
if not DB_URL:
    print("ERROR: DATABASE_URL environment variable is not set.")
    print("Set it before running, e.g.:")
    print('  PowerShell:  $env:DATABASE_URL="postgresql://postgres:pw@host:5432/postgres?sslmode=require"')
    print('  bash/zsh:    export DATABASE_URL="postgresql://postgres:pw@host:543:5432/postgres?sslmode=require"')
    sys.exit(1)

engine = create_engine(DB_URL, pool_pre_ping=True)

def ensure_schema_exists(schema_name: str):
    ddl = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    print(f"Schema '{schema_name}' ensured.")

def load_csv_to_table(csv_path: str, table_name: str, schema: str):
    full_path = os.path.join(DATA_DIR, csv_path)
    if not os.path.exists(full_path):
        print(f"SKIP: {full_path} not found.")
        return 0
    print(f"Loading {full_path} -> {schema}.{table_name} ...")
    # Read with pandas
    df = pd.read_csv(full_path, parse_dates=True, low_memory=False)
    # Clean column names (strip)
    df.columns = [c.strip() for c in df.columns]
    # Optional: cast boolean/numeric if needed (left as-is)
    # Write to Postgres
    # Use method='multi' for multi-row INSERTs, and chunksize to avoid memory spikes.
    try:
        df.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False,
                  method='multi', chunksize=5000)
        print(f"Loaded {len(df)} rows into {schema}.{table_name}")
        return len(df)
    except OperationalError as e:
        print("OperationalError while writing to DB:", e)
        raise
    except Exception as e:
        print("Error while writing to DB:", e)
        raise

def run_counts(schema: str, tables):
    print("\nVerifying row counts in DB:")
    with engine.connect() as conn:
        for t in tables:
            try:
                cnt = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{t}")).scalar()
                print(f"  {schema}.{t}: {cnt}")
            except Exception as e:
                print(f"  {schema}.{t}: ERROR ({e})")

def main():
    print("ETL start:", time.strftime("%Y-%m-%d %H:%M:%S"))
    # test connection
    try:
        with engine.connect() as conn:
            ver = conn.execute(text("SELECT version()")).scalar()
            print("Connected to:", ver.split("\n")[0])
    except Exception as e:
        print("ERROR: cannot connect to database. Full error:")
        print(e)
        sys.exit(1)

    ensure_schema_exists(ECOM_SCHEMA)

    loaded = {}
    for table, csvfile in CSV_MAP.items():
        try:
            n = load_csv_to_table(csvfile, table, ECOM_SCHEMA)
            loaded[table] = n
        except Exception as e:
            print(f"Failed to load {table}: {e}")
            # continue loading other tables but record failure
            loaded[table] = None

    run_counts(ECOM_SCHEMA, list(CSV_MAP.keys()))

    print("\nETL summary:")
    for t, c in loaded.items():
        print(f"  {t}: {c}")

    print("ETL finished:", time.strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    main()
