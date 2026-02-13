# MynaDB Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Configuration Layer                            │
├─────────────────────────────────────────────────────────────────┤
│  config.toml → Config → Components (ports, paths, etc)           │
│  CLI: mynadb [-c config.toml] [standalone|meta|data|router|admin]│
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                        Server Layer                              │
├──────────────┬──────────────┬──────────────┬────────────────────┤
│  HTTP REST   │ Arrow Flight │ MySQL Wire   │ OTLP (gRPC/HTTP)  │
│  (Port 7000) │ (Port 7001)  │ (Port 7002)  │ (Port 4317/4318)  │
└──────────────┴──────────────┴──────────────┴────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Processing Layer                            │
├──────────────┬──────────────┬──────────────┬────────────────────┤
│  DataCache   │ QueryEngine  │  Compactor   │  WAL               │
│  (60s flush) │ (DataFusion) │ (LSM-tree)   │  (Write-ahead log) │
└──────────────┴──────────────┴──────────────┴────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Storage Layer                               │
├─────────────────────────────────────────────────────────────────┤
│  Local filesystem, S3, MinIO, or Alibaba OSS                     │
│  ├── {catalog}/                                                  │
│  │   └── {schema}/                                               │
│  │       └── {table}/                                            │
│  │           └── date=YYYY-MM-DD/                                │
│  │               ├── L0_*.parquet   (recent writes)              │
│  │               ├── L1_*.parquet   (compacted)                  │
│  │               └── L2_*.parquet   (heavily compacted)          │
│  └── _system/                                                    │
│      └── series_stats/                                           │
└─────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Configuration System
**Location**: `src/config.rs`

**Responsibilities**:
- Load TOML configuration files via `-c` flag (default: `config.toml`)
- Provide default values for all settings
- Validate settings
- Expose typed configuration to components

**Key Settings**:
- Server ports (HTTP: 7000, Flight: 7001, MySQL: 7002)
- Data directory paths
- Object store backend (local, S3, MinIO, OSS)
- Schema extraction strategy (ServiceAsSchema, ServiceAsTable)
- Cache/flush intervals
- Compaction thresholds

### 2. HTTP REST API
**Location**: `src/services/api.rs` (routes defined in `src/main.rs`)
**Port**: Configurable (default: 7000)

**Endpoints**:
- `POST /write/:db` - Write data (InfluxDB line protocol)
- `GET/POST /query/:db` - SQL queries
- `GET /explain/:db` - Query execution plan
- `GET /analyze/:db` - Query analysis (partitions, files)
- `GET /tables/:db` - List tables
- `GET /schemas/:db` - List schemas
- `GET /schema/:db/:schema/:table` - Table schema
- `GET /partitions/:catalog/:schema/:table` - List partitions
- `DELETE /partitions/:catalog/:schema/:table/:date` - Delete partition
- `POST /compact/:db` - Trigger compaction
- `POST /flush` - Manual cache flush
- `GET /health` - Health check
- `GET /stats` - Write cache statistics

### 3. Arrow Flight API
**Location**: `src/services/flight_service.rs`
**Port**: Configurable (default: 7001)

**Features**:
- DataFusion-cli compatible
- Streaming query results
- Database context (`USE database`)
- Schema introspection

### 4. MySQL Wire Protocol
**Location**: `src/services/mysql_service.rs`
**Port**: Configurable (default: 7002)

**Features**:
- MySQL-compatible client connections
- Standard SQL queries via MySQL clients
- Enabled via `mysql_enabled = true` in config

### 5. DataCache
**Location**: `src/storage/cache/`

**Responsibilities**:
- In-memory buffering of writes
- Background flushing to Parquet (default: every 60 seconds)
- Batch processing for efficiency
- Memtable for immediate query access to unflushed data

**Configuration**:
- `cache.flush_interval_secs` - Flush frequency (default: 60)
- `cache.max_size_mb` - Max memory usage (default: 4096)

### 6. Write-Ahead Log (WAL)
**Location**: `src/storage/wal.rs`

**Responsibilities**:
- Durability for in-memory data before flush
- Crash recovery
- Configurable sync behavior

**Configuration**:
- `wal.enabled` - Enable/disable WAL (default: true)
- `wal.sync_on_write` - Sync to disk on every write (default: false)
- `wal.rotate_size_mb` - WAL file rotation size (default: 128)

### 7. QueryEngine
**Location**: `src/query/query_engine/`

**Responsibilities**:
- SQL query execution via Apache DataFusion
- Automatic table registration from Parquet files
- Schema discovery from object storage
- Custom UDFs (time_bucket, first_value, last_value, forecasting)
- Predicate pushdown and partition pruning

**Features**:
- Standard SQL with time series extensions
- Columnar execution with Arrow format
- Parallel query processing
- Query result caching

### 8. Compactor
**Location**: `src/jobs/compactor/`

**Responsibilities**:
- LSM-tree style multi-level compaction
- Merge small Parquet files into larger ones
- Date-based full compaction for yesterday's partitions

**Configuration**:
- `compaction.min_files` - L0→L1 trigger threshold (default: 5)
- `compaction.target_file_size_mb` - Target file size after compaction (default: 500)
- `compaction.max_level` - Maximum compaction depth (default: 5)
- `compaction.max_concurrent_compactions` - Parallel compaction limit (default: 8)

### 9. Retention Manager
**Location**: `src/jobs/retention_manager.rs`

**Responsibilities**:
- Automatic deletion of old date partitions
- Configurable retention period per database/table

**Configuration**:
- `retention.enabled` - Enable/disable (default: true)
- `retention.default_days` - Default retention period (default: 7)
- `retention.interval_secs` - Check frequency (default: 3600)

## Data Flow

### Write Path
```
Client → HTTP POST /write/:db
    ↓
Line Protocol Parser (src/storage/line_protocol.rs)
    ↓
WAL (src/storage/wal.rs) → Write-ahead log for durability
    ↓
DataCache (in-memory) → Immediately queryable via Memtable
    ↓
Background Flush (60s) → Parquet Files (L0, date-partitioned)
    ↓
Compactor (background) → Merged Files (L1, L2, ...)
```

### Query Path
```
Client → HTTP POST /query or Flight DoGet or MySQL
    ↓
QueryEngine (DataFusion) with custom analyzers & optimizers
    ↓
Union Provider (Memtable + Parquet files, all levels)
    ↓
Predicate Pushdown → Partition Pruning → Row Group Pruning
    ↓
Columnar Processing (Arrow format)
    ↓
Results → Client (JSON, Arrow, or MySQL wire protocol)
```

## Storage Structure

### Catalog/Schema/Table Organization

```
data/
└── {catalog}/                  # Database name from URI /write/{db}
    └── {schema}/               # From "service" tag (or "default")
        └── {table}/            # Measurement name
            └── date=YYYY-MM-DD/  # Date partition
                ├── L0_*.parquet    # Recent writes
                ├── L1_*.parquet    # First compaction level
                └── L2_*.parquet    # Higher compaction levels
```

**Schema extraction** is configured via `[schema]` in config.toml:
- `strategy` - `service_as_schema` (default) or `service_as_table`
- `tag_keys` - Array of tag keys to extract schema from (e.g., `["service", "hierarchy_2"]`)
- `prefix_numeric` - Prefix numeric schemas (e.g., `service=1` → `service_1`)
- `default_name` - Default schema when tag is missing (default: `"default"`)

## Performance Characteristics

### Write Performance
- **Buffered Writes**: In-memory cache before flush
- **WAL Durability**: Optional sync-on-write for crash safety
- **Batch Flush**: 60-second intervals (configurable)
- **Parallel Compaction**: Background compaction doesn't block writes

### Read Performance
- **Columnar Format**: Parquet for efficient scanning
- **Memtable**: Unflushed data is immediately queryable
- **Predicate Pushdown**: Filter early in DataFusion (Parquet, page, bloom)
- **Partition Pruning**: Skip irrelevant date partitions
- **Query Cache**: Cache results of slow queries
- **Level-Based Reading**: Recent data in L0, older in L1+

### Tuning Parameters

| Workload | Cache Flush | Compaction | File Size |
|----------|-------------|------------|-----------|
| Write-Heavy | 120s | min_files=15 | 1GB |
| Read-Heavy | 15s | min_files=5 | 200MB |
| Balanced | 60s | min_files=10 | 500MB |

## Configuration Examples

### Development
```toml
[storage]
data_dir = "./data"

[cache]
flush_interval_secs = 60
max_size_mb = 4096
```

### Production (High Throughput)
```toml
[storage]
data_dir = "/data0/mynadb/data"  # SSD/NVMe

[cache]
flush_interval_secs = 120
max_size_mb = 8192  # 8GB

[compaction]
min_files = 25
target_file_size_mb = 1024
```

### Production (Low Latency)
```toml
[storage]
data_dir = "/data0/mynadb/data"

[cache]
flush_interval_secs = 15
max_size_mb = 2048

[compaction]
min_files = 5
target_file_size_mb = 200
```

## Cluster Mode

MynaDB supports a distributed cluster mode with the following node roles:

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Router  │────→│   Meta   │←────│  Admin   │
│  Node    │     │  Cluster │     │  Console │
└────┬─────┘     │ (Raft)   │     └──────────┘
     │           └──────────┘
     │                ↑
     ↓                │
┌──────────┐   ┌──────────┐
│  Data    │   │  Data    │
│  Node 1  │   │  Node 2  │
└──────────┘   └──────────┘
```

- **Meta Nodes**: Raft consensus for metadata, node registry, leader election
- **Data Nodes**: Storage engine, query execution, compaction
- **Router Nodes**: Client protocols, query/write routing
- **Admin Node**: Web UI for cluster management

See `architecture-cluster.md` for full cluster architecture details.

## Monitoring

### Key Metrics to Monitor
1. **Cache Size**: Current memory usage
2. **Flush Duration**: Time to write to disk
3. **Compaction Frequency**: Files compacted per hour
4. **Query Latency**: P50, P95, P99 response times
5. **WAL Size**: Write-ahead log disk usage

### Endpoints
- `GET /health` - Health check
- `GET /stats` - Write cache statistics
- `GET /memory` - Memory usage stats
- `GET /debug/heap` - Heap profile (pprof compatible)
- `GET /debug/cpu` - CPU profile (pprof compatible)

## Dependencies

### Core
- **tokio**: Async runtime
- **axum**: HTTP framework
- **tonic**: gRPC framework

### Data
- **arrow**: Columnar format
- **parquet**: Parquet I/O
- **datafusion**: SQL engine
- **arrow-flight**: Arrow RPC
- **object_store**: Storage abstraction (local, S3, MinIO, OSS)

### Infrastructure
- **serde**: Serialization
- **clap**: CLI argument parsing
- **tracing**: Structured logging
