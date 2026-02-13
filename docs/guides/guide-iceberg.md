# Apache Iceberg Integration Guide

Expose MynaDB data as Apache Iceberg tables for Spark, Trino, DuckDB, and Flink. Zero-copy, zero-ETL — external engines read MynaDB's actual Parquet files.

## Quick Start

### 1. Enable the Feature

Add to your `config.toml`:

```toml
[iceberg]
enabled = true
```

### 2. Write Some Data

```bash
curl -X POST "http://localhost:7000/write/mydb" \
  -d 'cpu,host=server01 usage=42.5
cpu,host=server02 usage=38.1
memory,host=server01 total=16384,used=12000'
```

### 3. Wait for Sync

By default, Iceberg metadata syncs every 300 seconds. You can check the REST Catalog immediately:

```bash
# List namespaces
curl http://localhost:7000/iceberg/v1/namespaces

# List tables
curl http://localhost:7000/iceberg/v1/namespaces/default/tables

# Load table metadata
curl http://localhost:7000/iceberg/v1/namespaces/default/tables/cpu
```

### 4. Query from Spark

```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.mynadb", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.mynadb.type", "rest") \
    .config("spark.sql.catalog.mynadb.uri", "http://mynadb-host:7000/iceberg") \
    .getOrCreate()

spark.sql("SELECT * FROM mynadb.default.cpu").show()
```

## Configuration

### Full Configuration Reference

```toml
[iceberg]
# Enable Iceberg integration (default: false)
enabled = true

# Enable REST Catalog API on main HTTP server (default: true)
rest_catalog_enabled = true

# URL prefix for REST Catalog API (default: "/iceberg")
rest_catalog_prefix = "/iceberg"

# Directory for Iceberg metadata files (default: "iceberg")
# Relative to storage.data_dir
metadata_dir = "iceberg"

# Background sync interval in seconds (default: 300)
# Set to 0 to disable background sync (manual only)
sync_interval_secs = 300

# Sync metadata on every flush (default: true)
# More up-to-date but slightly more I/O overhead
sync_on_flush = true
```

## REST Catalog API

MynaDB implements the [Iceberg REST Catalog specification](https://iceberg.apache.org/spec/#rest-catalog). All endpoints are mounted under `/iceberg/v1/`.

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/iceberg/v1/config` | GET | Server configuration (warehouse defaults) |
| `/iceberg/v1/namespaces` | GET | List all namespaces (MynaDB schemas) |
| `/iceberg/v1/namespaces/:ns` | GET | Get namespace details |
| `/iceberg/v1/namespaces/:ns/tables` | GET | List tables in namespace |
| `/iceberg/v1/namespaces/:ns/tables/:table` | GET | Load full table metadata |
| `/iceberg/v1/namespaces/:ns/tables/:table` | HEAD | Check if table exists |

### Example Responses

**List namespaces:**
```bash
curl http://localhost:7000/iceberg/v1/namespaces
```
```json
{
  "namespaces": [
    ["mydb", "default"],
    ["mydb", "production"],
    ["otel", "default"]
  ]
}
```

**List tables:**
```bash
curl http://localhost:7000/iceberg/v1/namespaces/default/tables
```
```json
{
  "identifiers": [
    {"namespace": ["default"], "name": "cpu"},
    {"namespace": ["default"], "name": "memory"},
    {"namespace": ["default"], "name": "disk"}
  ]
}
```

**Load table metadata:**
```bash
curl http://localhost:7000/iceberg/v1/namespaces/default/tables/cpu
```
```json
{
  "metadata-location": "iceberg/mydb/default/cpu/metadata/v1.metadata.json",
  "metadata": {
    "format-version": 2,
    "table-uuid": "...",
    "location": "./data/store/mydb/default/cpu",
    "schemas": [{
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "time", "required": true, "type": "timestamptz"},
        {"id": 2, "name": "date", "required": true, "type": "date"},
        {"id": 3, "name": "usage", "required": false, "type": "double"},
        {"id": 4, "name": "host", "required": false, "type": "string"}
      ]
    }],
    "partition-specs": [{
      "spec-id": 0,
      "fields": [{"source-id": 2, "field-id": 1000, "name": "date", "transform": "identity"}]
    }],
    "current-snapshot-id": 1707753600000,
    "snapshots": [...]
  }
}
```

## External Engine Integration

### Apache Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.catalog.mynadb", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.mynadb.type", "rest") \
    .config("spark.sql.catalog.mynadb.uri", "http://mynadb-host:7000/iceberg") \
    .getOrCreate()

# Query MynaDB data
df = spark.sql("""
    SELECT host, avg(usage) as avg_cpu
    FROM mynadb.default.cpu
    WHERE date >= '2026-02-10'
    GROUP BY host
""")
df.show()
```

### Trino

```sql
-- In Trino catalog configuration (etc/catalog/mynadb.properties):
-- connector.name=iceberg
-- iceberg.catalog.type=rest
-- iceberg.rest-catalog.uri=http://mynadb-host:7000/iceberg

SELECT host, avg(usage) as avg_cpu
FROM mynadb.default.cpu
WHERE date >= DATE '2026-02-10'
GROUP BY host;
```

### DuckDB

```sql
INSTALL iceberg;
LOAD iceberg;

-- Query via REST catalog
SELECT *
FROM iceberg_scan('http://mynadb-host:7000/iceberg/v1/namespaces/default/tables/cpu');
```

### Apache Flink

```sql
CREATE CATALOG mynadb WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://mynadb-host:7000/iceberg'
);

USE CATALOG mynadb;

SELECT * FROM `default`.cpu
WHERE `date` >= '2026-02-10';
```

### pyiceberg (Python)

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("mynadb", **{
    "type": "rest",
    "uri": "http://mynadb-host:7000/iceberg",
})

# List all tables
for ns in catalog.list_namespaces():
    for table_id in catalog.list_tables(ns):
        print(table_id)

# Load and scan a table
table = catalog.load_table("default.cpu")
df = table.scan(
    row_filter="date >= '2026-02-10'",
    selected_fields=("time", "host", "usage"),
).to_pandas()
print(df.head())
```

## Namespace Mapping

MynaDB's three-level hierarchy maps to Iceberg namespaces:

| MynaDB | Iceberg |
|--------|---------|
| Database (catalog) | Warehouse |
| Schema | Namespace |
| Table | Table |
| `date=YYYY-MM-DD/` partition | Iceberg date partition |
| Arrow schema | Iceberg v2 schema |
| Parquet files (L0-L4) | Data files in manifests |

### Namespace Format

Namespaces in the REST API can be specified as:
- `default` — schema name only (auto-discovers catalog)
- `mydb.default` — explicit `catalog.schema` format

## Metadata Files

Iceberg metadata files are stored under `{data_dir}/iceberg/`:

```
data/iceberg/
└── mydb/
    └── default/
        └── cpu/
            └── metadata/
                ├── v1.metadata.json              # Latest table metadata
                ├── v2.metadata.json              # After second sync
                ├── manifest-1707753600000.json    # Data file manifest
                └── snap-1707753600000-manifest-list.json
```

These files are generated by the background sync job and never modify your Parquet data files.

## Sync Behavior

### Background Sync

The sync job runs every `sync_interval_secs` (default: 300s):

1. Scans all `store/{catalog}/{schema}/{table}/` directories
2. Reads Arrow schema from the first Parquet file in each table
3. Scans `date=*` partitions for `.parquet` files
4. Creates Iceberg manifests, manifest lists, and snapshots
5. Writes versioned `vN.metadata.json` files
6. Updates in-memory metadata cache

### Sync on Flush

When `sync_on_flush = true`, metadata is also updated after each background flush cycle, ensuring near-real-time visibility in external engines (within the flush interval, default 60s).

### Manual Sync

Currently, sync runs automatically on the configured interval. To force an immediate sync, reduce `sync_interval_secs` temporarily or restart MynaDB.

## How It Works with Compaction

When MynaDB compacts Parquet files (L0 -> L1 -> L2), the next sync cycle detects the new file layout and creates a fresh snapshot. Old snapshots remain in the metadata, enabling time-travel queries in engines that support it.

## Limitations

- **Read-only**: External engines can read but not write MynaDB tables via Iceberg
- **No schema evolution**: Schema changes in MynaDB require a full metadata rebuild
- **Local storage only**: Currently generates metadata for local filesystem storage
- **JSON metadata**: Manifest files use JSON encoding (not Avro) for simplicity
- **No partition pruning stats**: Column-level min/max statistics are not yet populated in manifests

## Troubleshooting

### Table shows "metadata not yet generated"

The background sync hasn't run yet. Wait for `sync_interval_secs` or check logs:

```bash
grep "Iceberg" logs/mynadb.log
```

### Empty namespace list

Ensure data has been written and flushed to Parquet. In-memory cache data is not visible to Iceberg until flushed.

### Schema mismatch

If MynaDB table schema changes (new columns added), the Iceberg metadata will be stale until the next sync cycle. The sync reads the schema from the first Parquet file found, so very old files may have outdated schemas.

### External engine can't connect

1. Check that `[iceberg] enabled = true`
2. Verify the REST endpoint: `curl http://localhost:7000/iceberg/v1/config`
3. Ensure the external engine uses the correct URI prefix (`/iceberg`)
