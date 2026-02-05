# Stats Service Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────┐
│                   Configuration Layer                    │
├─────────────────────────────────────────────────────────┤
│  config.toml → Config → Components (ports, paths, etc)  │
└─────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│                      Server Layer                        │
├──────────────────────┬──────────────────────────────────┤
│   HTTP REST API      │   Arrow Flight gRPC              │
│   (Port: 7000)       │   (Port: 7001)                   │
└──────────────────────┴──────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│                     Processing Layer                     │
├──────────────┬──────────────┬──────────────┬────────────┤
│  DataCache   │ QueryEngine  │  Compactor   │  Series    │
│  (30s flush) │ (DataFusion) │ (LSM-tree)   │  Tracker   │
└──────────────┴──────────────┴──────────────┴────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│                     Storage Layer                        │
├─────────────────────────────────────────────────────────┤
│  /data0/data/                                            │
│  ├── database1/                                          │
│  │   ├── measurement1/                                   │
│  │   │   ├── L0_*.parquet   (recent writes)             │
│  │   │   ├── L1_*.parquet   (compacted)                 │
│  │   │   └── L2_*.parquet   (heavily compacted)         │
│  └── _system/                                            │
│      └── series_stats/                                   │
│          └── database1.parquet                           │
└─────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Configuration System
**Location**: `src/config.rs`

**Responsibilities**:
- Load TOML configuration files
- Provide default values
- Validate settings
- Expose typed configuration to components

**Key Settings**:
- Server ports (HTTP: 7000, Flight: 7001)
- Data directory paths
- Cache/flush intervals
- Compaction thresholds

### 2. HTTP REST API
**Location**: `src/main.rs`
**Port**: Configurable (default: 7000)

**Endpoints**:
- `POST /write/:db` - Write data (InfluxDB line protocol)
- `GET /query/:db` - SQL queries
- `POST /databases` - Create database
- `GET /databases` - List databases
- `POST /compact/:db` - Trigger compaction
- `GET /series_stats/:db` - Series cardinality stats

### 3. Arrow Flight API
**Location**: `src/flight_service.rs`
**Port**: Configurable (default: 7001)

**Features**:
- DataFusion-cli compatible
- Streaming query results
- Database context (`USE database`)
- Schema introspection

### 4. DataCache
**Location**: `src/cache.rs`

**Responsibilities**:
- In-memory buffering of writes
- Background flushing to Parquet
- Batch processing for efficiency

**Configuration**:
- `cache.flush_interval_secs` - Flush frequency
- `cache.max_size_mb` - Max memory usage

### 5. QueryEngine
**Location**: `src/query_engine.rs`

**Responsibilities**:
- SQL query execution via DataFusion
- Automatic table registration
- Schema discovery

**Features**:
- Supports standard SQL
- Columnar execution
- Parallel query processing

### 6. Compactor
**Location**: `src/compactor.rs`

**Responsibilities**:
- LSM-tree style compaction
- Merge small files into larger ones
- Multi-level compaction strategy

**Configuration**:
- `compaction.min_files` - Trigger threshold
- `compaction.max_file_size_mb` - File size limit
- `compaction.target_file_size_mb` - Target after compaction
- `compaction.max_level` - Compaction depth

### 7. Series Tracker
**Location**: `src/series_tracker.rs`

**Responsibilities**:
- Track unique time series
- Monitor cardinality
- Detect high-cardinality issues

**Configuration**:
- `series_tracker.flush_interval_secs` - Stats update frequency
- `series_tracker.stats_dir` - Stats storage location

## Data Flow

### Write Path
```
Client → HTTP POST /write
    ↓
Line Protocol Parser
    ↓
Points → DataCache (in-memory)
    ↓
Background Flush (30s) → Parquet Files (L0)
    ↓
Compactor (triggered by file count) → Merged Files (L1, L2, ...)
```

### Query Path
```
Client → HTTP GET /query or Flight DoGet
    ↓
QueryEngine (DataFusion)
    ↓
Scan Parquet Files (all levels)
    ↓
Columnar Processing
    ↓
Results → Client (JSON or Arrow)
```

## Performance Characteristics

### Write Performance
- **Buffered Writes**: In-memory cache before flush
- **Batch Flush**: 30-second intervals (configurable)
- **Parallel Compaction**: Background compaction doesn't block writes

### Read Performance
- **Columnar Format**: Parquet for efficient scanning
- **Predicate Pushdown**: Filter early in DataFusion
- **Level-Based Reading**: Recent data in L0, older in L1+

### Tuning Parameters

| Workload | Cache Flush | Compaction | File Size |
|----------|-------------|------------|-----------|
| Write-Heavy | 60s | min_files=15 | 2GB |
| Read-Heavy | 20s | min_files=5 | 512MB |
| Balanced | 30s | min_files=10 | 1GB |

## Configuration Examples

### Development
```toml
[storage]
data_dir = "./data"

[cache]
flush_interval_secs = 30
max_size_mb = 1024
```

### Production (High Throughput)
```toml
[storage]
data_dir = "/data0/data"  # SSD/NVMe

[cache]
flush_interval_secs = 60
max_size_mb = 4096  # 4GB

[compaction]
min_files = 20
target_file_size_mb = 2048  # 2GB files
```

### Production (Low Latency)
```toml
[storage]
data_dir = "/data0/data"

[cache]
flush_interval_secs = 15
max_size_mb = 512

[compaction]
min_files = 5
target_file_size_mb = 512
```

## Monitoring

### Key Metrics to Monitor
1. **Cache Size**: Current memory usage
2. **Flush Duration**: Time to write to disk
3. **Compaction Frequency**: Files compacted per hour
4. **Query Latency**: P50, P95, P99 response times
5. **Series Cardinality**: Unique time series count

### Log Messages
- `🔄 Cache flushed: X batches, Y points`
- `🗜️  Compacted L0 files: X → Y`
- `✅ Loaded configuration from: config.toml`

## File Structure
```
stats-service/
├── src/
│   ├── main.rs              # Server & routing
│   ├── config.rs            # Configuration system ⭐ NEW
│   ├── cache.rs             # In-memory cache
│   ├── query_engine.rs      # DataFusion integration
│   ├── compactor.rs         # LSM compaction
│   ├── series_tracker.rs    # Cardinality tracking
│   ├── flight_service.rs    # Arrow Flight RPC
│   ├── line_protocol.rs     # InfluxDB parser
│   ├── parquet_writer.rs    # Parquet I/O
│   └── database_manager.rs  # DB operations
├── config.toml              # Default config ⭐ NEW
├── config.production.toml   # Production config ⭐ NEW
├── CONFIG.md                # Config documentation ⭐ NEW
└── ARCHITECTURE.md          # This file ⭐ NEW
```

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

### Config ⭐ NEW
- **config**: Configuration management
- **toml**: TOML parsing
- **serde**: Serialization

## Future Enhancements
- [ ] Authentication & authorization
- [ ] Distributed deployment
- [ ] Replication & clustering
- [ ] Hot config reload
- [ ] Metrics endpoint (Prometheus)
- [ ] Write-ahead log (WAL)
