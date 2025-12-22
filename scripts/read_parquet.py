#!/usr/bin/env python3
"""
Read and display Parquet files created by Stats TSDB
Usage: python3 read_parquet.py <parquet_file>
"""

import sys
import os

try:
    import pyarrow.parquet as pq
    import pandas as pd
except ImportError:
    print("Error: PyArrow and Pandas are required")
    print("Install with: pip install pyarrow pandas")
    sys.exit(1)


def read_parquet_file(filepath):
    """Read and display a Parquet file"""
    if not os.path.exists(filepath):
        print(f"Error: File not found: {filepath}")
        return

    print(f"\n📄 Reading: {filepath}")
    print("=" * 80)

    # Read the Parquet file
    table = pq.read_table(filepath)

    # Display schema
    print("\n📋 Schema:")
    print(table.schema)

    # Convert to pandas and display data
    df = table.to_pandas()

    print(f"\n📊 Data ({len(df)} rows):")
    print(df.to_string())

    # Display statistics for numeric columns
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    if len(numeric_cols) > 0:
        print("\n📈 Statistics:")
        print(df[numeric_cols].describe())


def list_and_read_db(db_path):
    """List all Parquet files in a database directory"""
    if not os.path.exists(db_path):
        print(f"Error: Database path not found: {db_path}")
        return

    parquet_files = [f for f in os.listdir(db_path) if f.endswith('.parquet')]

    if not parquet_files:
        print(f"No Parquet files found in {db_path}")
        return

    print(f"\n📁 Found {len(parquet_files)} Parquet file(s) in {db_path}:")
    for i, filename in enumerate(parquet_files, 1):
        print(f"  {i}. {filename}")

    print("\n" + "=" * 80)

    for filename in parquet_files:
        filepath = os.path.join(db_path, filename)
        read_parquet_file(filepath)
        print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Read single file:    python3 read_parquet.py <parquet_file>")
        print("  Read all in DB:      python3 read_parquet.py ./data/<dbname>")
        print("\nExample:")
        print("  python3 read_parquet.py ./data/testdb/cpu_1703251234567.parquet")
        print("  python3 read_parquet.py ./data/testdb")
        sys.exit(1)

    path = sys.argv[1]

    if os.path.isdir(path):
        list_and_read_db(path)
    elif os.path.isfile(path):
        read_parquet_file(path)
    else:
        print(f"Error: Invalid path: {path}")
        sys.exit(1)