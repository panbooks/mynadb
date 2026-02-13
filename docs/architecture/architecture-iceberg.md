# Apache Iceberg Table Format Architecture

## Overview

MynaDB exposes its Parquet data as Apache Iceberg v2 tables, enabling external query engines (Apache Spark, Trino, DuckDB, Apache Flink) to read MynaDB data without any data copying or ETL pipelines. A background sync job generates Iceberg metadata from existing Parquet files, and a REST Catalog API serves that metadata to external tools.

This turns MynaDB into a data lakehouse node: it ingests high-frequency time-series data and simultaneously makes it queryable by the entire lakehouse ecosystem.

## Design Goals

1. **Read-only bridge**: Generate metadata without modifying existing Parquet files
2. **Zero-copy**: External engines read MynaDB's actual Parquet files in-place
3. **Incremental**: Only scan for new files on each sync cycle
4. **Standard API**: Full Iceberg REST Catalog spec for maximum compatibility
5. **Backward compatible**: Disabled by default; zero overhead when not enabled

## Architecture Diagram

```
                    ┌──────────────────────────────────────────────────────┐
                    │               MynaDB WRITE PATH (existing)           │
                    │                                                      │
  Line Protocol ────┼──► DataCache ──► Background Flush ──► Parquet Files  │
  OTLP              │                                       │              │
  Prometheus        │                   store/{catalog}/{schema}/{table}/  │
                    │                     date=YYYY-MM-DD/                 │
                    │                       L0_*.parquet                   │
                    │                       L1_*.parquet                   │
                    └───────────────────────┼──────────────────────────────┘
                                            │
                    ┌───────────────────────┼──────────────────────────────┐
                    │         METADATA SYNC JOB (background)               │
                    │                       │                              │
                    │    SnapshotBuilder     ▼                             │
                    │    1. Scan table dir for Parquet files               │
                    │    2. Read Arrow schema from first file              │
                    │    3. Convert to Iceberg v2 schema                   │
                    │    4. Build manifest (file → entry mapping)          │
                    │    5. Build manifest list                            │
                    │    6. Create snapshot                                │
                    │    7. Write/update table metadata JSON               │
                    │                       │                              │
                    │    Output:            ▼                              │
                    │    iceberg/{catalog}/{schema}/{table}/metadata/      │
                    │      v1.metadata.json                                │
                    │      manifest-{snap_id}.json                         │
                    │      snap-{snap_id}-manifest-list.json               │
                    │                                                      │
                    └───────────────────────┼──────────────────────────────┘
                                            │
                    ┌───────────────────────┼──────────────────────────────┐
                    │          REST CATALOG API (/iceberg/v1/*)            │
                    │                       │                              │
  Spark SQL ────────┼──► GET /v1/config                                   │
  Trino             │──► GET /v1/namespaces                               │
  DuckDB            │──► GET /v1/namespaces/:ns                           │
  Flink             │──► GET /v1/namespaces/:ns/tables                    │
  pyiceberg         │──► GET /v1/namespaces/:ns/tables/:table             │
                    │         │                                           │
                    │         ▼                                           │
                    │    MynaDbIcebergCatalog                              │
                    │    - Scans store/ directory for namespaces/tables    │
                    │    - Loads metadata from iceberg/ directory          │
                    │    - Caches metadata in memory                      │
                    │                                                      │
                    └──────────────────────────────────────────────────────┘
```

## Module Structure

```
src/iceberg/
├── mod.rs                          # Top-level module
├── catalog/
│   ├── mod.rs
│   ├── mynadb_catalog.rs           # MynaDB ↔ Iceberg namespace mapping
│   │                               #   - list_namespaces()
│   │                               #   - list_tables()
│   │                               #   - get_table_metadata() (cached)
│   │                               #   - table_exists()
│   └── rest_catalog.rs             # Axum REST Catalog API handlers
│                                   #   - config_handler
│                                   #   - list_namespaces_handler
│                                   #   - get_namespace_handler
│                                   #   - list_tables_handler
│                                   #   - load_table_handler
│                                   #   - table_exists_handler
├── metadata/
│   ├── mod.rs
│   ├── schema.rs                   # Arrow → Iceberg v2 schema conversion
│   ├── partition_spec.rs           # Partition spec generation
│   ├── snapshot.rs                 # Snapshot creation (append/overwrite)
│   ├── manifest.rs                 # Manifest files (data file tracking)
│   ├── manifest_list.rs            # Manifest list (manifest tracking)
│   └── table_metadata.rs           # Iceberg v2 TableMetadata (top-level)
└── sync/
    ├── mod.rs
    ├── metadata_sync.rs            # Background sync job
    └── snapshot_builder.rs         # Parquet scanning + snapshot building
```

## Core Components

### 1. Schema Conversion (`metadata/schema.rs`)

Converts Arrow schemas (read from Parquet file metadata) to Iceberg v2 schemas:

| Arrow Type | Iceberg Type |
|-----------|--------------|
| `Boolean` | `boolean` |
| `Int8/16/32`, `UInt8/16/32` | `int` |
| `Int64`, `UInt64` | `long` |
| `Float16/32` | `float` |
| `Float64` | `double` |
| `Utf8`, `LargeUtf8` | `string` |
| `Binary`, `LargeBinary` | `binary` |
| `Date32`, `Date64` | `date` |
| `Timestamp(*, *)` | `timestamptz` |
| `Decimal128(p,s)` | `decimal(p,s)` |
| `Dictionary(_, V)` | recurse on V |

Each field gets a sequential integer ID starting at 1.

### 2. Partition Spec (`metadata/partition_spec.rs`)

Maps MynaDB's `date=YYYY-MM-DD/` directory partitioning to Iceberg partition specs:

| MynaDB | Iceberg Spec |
|--------|-------------|
| `date` column exists (Date32) | `identity(date)` transform |
| Only `time` column (Timestamp) | `day(time)` transform |
| Neither | Unpartitioned |

Partition field IDs start at 1000 per the Iceberg spec.

### 3. Table Metadata (`metadata/table_metadata.rs`)

The root metadata structure for an Iceberg v2 table:

```json
{
  "format-version": 2,
  "table-uuid": "...",
  "location": "store/mydb/default/cpu",
  "last-sequence-number": 3,
  "schemas": [{ "type": "struct", "schema-id": 0, "fields": [...] }],
  "current-schema-id": 0,
  "partition-specs": [{ "spec-id": 0, "fields": [...] }],
  "default-spec-id": 0,
  "current-snapshot-id": 1707753600000,
  "snapshots": [...],
  "snapshot-log": [...],
  "sort-orders": [{ "order-id": 0, "fields": [] }],
  "properties": { "write.format.default": "parquet", "created-by": "mynadb" }
}
```

### 4. Snapshot Builder (`sync/snapshot_builder.rs`)

Scans a MynaDB table directory and produces Iceberg metadata files:

```
Input:  store/mydb/default/cpu/
          date=2026-02-10/L0_aaa.parquet (1MB)
          date=2026-02-10/L1_bbb.parquet (5MB)
          date=2026-02-11/L0_ccc.parquet (2MB)

Output: iceberg/mydb/default/cpu/metadata/
          v1.metadata.json          (table metadata)
          manifest-170775360.json   (data file entries)
          snap-170775360-manifest-list.json
```

Process:
1. Read Arrow schema from first Parquet file found
2. Convert to Iceberg schema
3. Create partition spec from `date` or `time` column
4. Scan all `date=*` partition directories for `.parquet` files
5. Build manifest with one entry per data file
6. Build manifest list pointing to the manifest
7. Create snapshot with file/row/size summaries
8. Update table metadata and write versioned JSON files

### 5. Metadata Sync Job (`sync/metadata_sync.rs`)

Background task that runs every `sync_interval_secs`:

```
sync_all():
  for each catalog in store/:
    for each schema in catalog/:
      for each table in schema/:
        snapshot_builder.build_table_snapshot(catalog, schema, table)
        catalog_cache.update_metadata(...)
```

Also supports single-table sync for on-flush/on-compaction triggers.

### 6. REST Catalog API (`catalog/rest_catalog.rs`)

Implements the [Iceberg REST Catalog specification](https://iceberg.apache.org/spec/#rest-catalog):

| Endpoint | Method | Response |
|----------|--------|----------|
| `/iceberg/v1/config` | GET | Warehouse defaults |
| `/iceberg/v1/namespaces` | GET | List of `[catalog, schema]` pairs |
| `/iceberg/v1/namespaces/:ns` | GET | Namespace properties |
| `/iceberg/v1/namespaces/:ns/tables` | GET | Table identifiers |
| `/iceberg/v1/namespaces/:ns/tables/:table` | GET | Full table metadata |
| `/iceberg/v1/namespaces/:ns/tables/:table` | HEAD | Existence check |

### 7. MynaDB Catalog (`catalog/mynadb_catalog.rs`)

In-memory catalog that maps between MynaDB and Iceberg namespaces:

| MynaDB Concept | Iceberg Concept |
|---------------|-----------------|
| catalog (database) | warehouse |
| schema | namespace |
| table | table |
| `date=YYYY-MM-DD/` partition | `day(time)` or `identity(date)` partition |
| Arrow schema (from Parquet) | Iceberg v2 schema |
| Parquet files (L0-L4) | Data files in manifests |

Metadata is cached with `parking_lot::RwLock<HashMap>` for concurrent read access.

## Integration Points

| File | Change |
|------|--------|
| `src/config.rs` | `IcebergConfig` struct with `[iceberg]` TOML section |
| `src/lib.rs` / `src/main.rs` | `pub mod iceberg;` |
| `src/storage/layout.rs` | `iceberg_dir()`, `iceberg_table_metadata_dir()` |
| `src/node/standalone.rs` | Initialize sync job + catalog on startup |
| `src/services/api.rs` | Mount Iceberg REST routes |

## Metadata File Layout

```
{data_dir}/
├── store/                          # Existing Parquet data (unchanged)
│   └── mydb/
│       └── default/
│           └── cpu/
│               ├── date=2026-02-10/
│               │   └── L0_abc.parquet
│               └── date=2026-02-11/
│                   └── L0_def.parquet
│
└── iceberg/                        # Generated Iceberg metadata
    └── mydb/
        └── default/
            └── cpu/
                └── metadata/
                    ├── v1.metadata.json           # Latest metadata (symlink-like)
                    ├── v2.metadata.json           # After 2nd sync
                    ├── manifest-17077536.json      # Snapshot 1 manifest
                    ├── snap-17077536-manifest-list.json
                    ├── manifest-17077540.json      # Snapshot 2 manifest
                    └── snap-17077540-manifest-list.json
```

## External Engine Integration

### Spark

```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.mynadb", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.mynadb.type", "rest") \
    .config("spark.sql.catalog.mynadb.uri", "http://mynadb-host:7000/iceberg") \
    .getOrCreate()

df = spark.sql("SELECT * FROM mynadb.default.cpu WHERE date >= '2026-02-10'")
```

### DuckDB

```sql
INSTALL iceberg;
LOAD iceberg;

SELECT * FROM iceberg_scan('http://mynadb-host:7000/iceberg/v1/namespaces/default/tables/cpu');
```

### pyiceberg

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("mynadb", type="rest", uri="http://mynadb-host:7000/iceberg")
table = catalog.load_table("default.cpu")
df = table.scan().to_pandas()
```

## Performance Considerations

- **Lazy schema reading**: Arrow schema is read from a single Parquet file per table, not all files
- **Incremental snapshots**: Each sync creates a new snapshot; old snapshots remain for time travel
- **In-memory catalog cache**: Metadata is cached after first load, invalidated on sync
- **Background sync**: Sync runs on a separate Tokio task, never blocks write or query paths
- **File estimation**: Record counts are estimated from file size to avoid opening every file
