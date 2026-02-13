# MynaDB - Feature Roadmap

## Current State

MynaDB is a clean, functional time series database with **~9,150 lines** of Rust code across 17 modules.

### Core Modules

| Module | Lines | Purpose |
|--------|-------|---------|
| `compactor.rs` | 2,014 | LSM-tree multi-level compaction |
| `query_engine.rs` | 1,192 | DataFusion SQL query execution |
| `api.rs` | 1,157 | REST API handlers |
| `wal.rs` | 722 | Write-ahead log for durability |
| `config.rs` | 667 | TOML configuration |
| `mysql_service.rs` | 550 | MySQL wire protocol |
| `main.rs` | 502 | Server startup and routing |
| `cache.rs` | 382 | In-memory write buffer |
| `database_manager.rs` | 375 | Database CRUD operations |
| `retention_manager.rs` | 313 | Automatic data retention |
| `flight_service.rs` | 311 | Arrow Flight gRPC API |
| `profiling.rs` | 214 | CPU/heap profiling (pprof) |
| `auth.rs` | 186 | Basic authentication |
| `parquet_writer.rs` | 178 | Parquet file I/O |
| `line_protocol.rs` | 175 | InfluxDB line protocol parser |
| `rate_limiter.rs` | 149 | I/O throttling for compaction |
| `logger.rs` | 65 | Structured logging setup |

### Current Features

- InfluxDB line protocol ingestion
- SQL queries via DataFusion
- Parquet storage with date partitioning
- LSM-tree compaction (L0 → L1 → L2...)
- Write-ahead log (WAL) for durability
- Arrow Flight and MySQL protocol support
- Automatic data retention
- Basic authentication

---

## Missing Features for Full-Featured TSDB

### 1. Indexing & Query Optimization (High Priority)

| Feature | Description | Benefit |
|---------|-------------|---------|
| Time-based indexing | Index on timestamp column | Fast range scans |
| Tag indexing | B-tree or bitmap index on tags | Fast tag filtering |
| Inverted index | Tag value → series ID mapping | Series discovery |
| Predicate push-down | Push filters to Parquet scan | Reduce I/O |
| Statistics-based pruning | Use Parquet min/max stats | Skip irrelevant files |

### 2. Aggregation & Downsampling (High Priority)

| Feature | Description | Benefit |
|---------|-------------|---------|
| Continuous queries | Auto-aggregate on write | Real-time rollups |
| Rollup tables | Pre-computed 1m/5m/1h/1d aggregates | Fast dashboard queries |
| `time_bucket()` function | SQL function for time grouping | Simplified queries |
| Retention per rollup | Keep raw 7d, 1m for 30d, 1h for 1y | Storage efficiency |

### 3. Cluster Mode (Planned)

| Feature | Description | Benefit |
|---------|-------------|---------|
| Sharding | Hash-based data distribution | Horizontal scaling |
| Consistent hashing | Stable shard assignment | Minimal rebalancing |
| Node discovery | Gossip or etcd-based | Auto cluster formation |
| Distributed queries | Scatter-gather execution | Query any node |
| Replication | N-way data copies | Fault tolerance |

### 4. Data Management

| Feature | Description | Benefit |
|---------|-------------|---------|
| Schema evolution | Add/drop columns online | Flexibility |
| Partition migration | Move data between nodes | Load balancing |
| Backup/restore | Point-in-time snapshots | Disaster recovery |
| Bulk import/export | CLI tools for data movement | Migration support |

### 5. Monitoring & Observability

| Feature | Description | Benefit |
|---------|-------------|---------|
| Internal metrics | TSDB monitors itself | Self-awareness |
| Slow query log | Log queries > N ms | Performance tuning |
| Query statistics | Cache hits, rows scanned | Optimization insights |
| Health endpoints | Detailed health checks | Operational visibility |

### 6. Advanced Query Features

| Feature | Description | Benefit |
|---------|-------------|---------|
| Window functions | `rate()`, `increase()`, moving avg | Time series analysis |
| Subqueries | Nested SELECT statements | Complex queries |
| JOINs | Cross-table queries | Data correlation |
| `EXPLAIN` | Query execution plan | Query optimization |
| `SHOW` commands | `SHOW TABLES`, `SHOW COLUMNS` | Schema discovery |

### 7. Write Path Improvements

| Feature | Description | Benefit |
|---------|-------------|---------|
| Batch write API | `/write/batch` endpoint | Higher throughput |
| Back-pressure | Reject writes when overloaded | Stability |
| Write deduplication | Detect duplicate points | Data integrity |
| Out-of-order writes | Handle late-arriving data | Flexibility |

---

## Recommended Implementation Order

### Phase 1: Query Performance
1. Predicate push-down to Parquet
2. Time-range pruning using Parquet statistics
3. `EXPLAIN` command for query analysis

### Phase 2: Aggregation
1. `time_bucket()` SQL function
2. Continuous aggregation framework
3. Rollup table management

### Phase 3: Cluster Mode
1. Sharding strategy design
2. Distributed query execution
3. Node discovery and membership
4. Data replication

### Phase 4: Operations
1. Internal metrics exposition
2. Backup/restore tools
3. Schema evolution support

---

## Architecture Principles

1. **Simple over complex** - Avoid premature abstraction
2. **Parquet-native** - Leverage Parquet's built-in features
3. **DataFusion-first** - Use DataFusion's optimizer
4. **Append-only** - Immutable files, compaction for cleanup
5. **Date-partitioned** - Natural time-based organization
