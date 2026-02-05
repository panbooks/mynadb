# Caching Layer Architecture

This document describes the architecture of MynaDB's caching layer, which consists of two main components:

1. **MetadataManager** - Centralized catalog metadata management
2. **FileCache** - Local disk cache for Parquet files from Object Store

## Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Query Engine                                    │
│                                                                              │
│  ┌─────────────────────────────┐      ┌─────────────────────────────────┐   │
│  │     MetadataManager         │      │         FileCache               │   │
│  │                             │      │                                 │   │
│  │  • Catalog/Schema/Table     │      │  • L1: Memory (hot files)      │   │
│  │  • Partition metadata       │      │  • L2: Local SSD (warm files)  │   │
│  │  • File statistics          │      │  • L3: Object Store (cold)     │   │
│  │  • JSON persistence         │      │  • LRU + partition-aware       │   │
│  └──────────────┬──────────────┘      └───────────────┬─────────────────┘   │
│                 │                                     │                      │
│                 ▼                                     ▼                      │
│  ┌─────────────────────────────┐      ┌─────────────────────────────────┐   │
│  │   catalog_snapshot.json     │      │   /var/cache/mynadb/parquet/    │   │
│  │   (Metadata Persistence)    │      │   (Local File Cache)            │   │
│  └─────────────────────────────┘      └───────────────┬─────────────────┘   │
│                                                       │                      │
└───────────────────────────────────────────────────────┼──────────────────────┘
                                                        │
                                                        ▼
                                       ┌─────────────────────────────────┐
                                       │        Object Store             │
                                       │   (S3, MinIO, Aliyun OSS)       │
                                       └─────────────────────────────────┘
```

## 1. MetadataManager

### Purpose

The MetadataManager eliminates expensive directory scanning operations during query planning. Instead of traversing the filesystem to discover catalogs, schemas, tables, partitions, and files, queries can look up this information from an in-memory index.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         MetadataManager (Trait)                          │
├─────────────────────────────────────────────────────────────────────────┤
│  Lifecycle:     load(), save(), rescan()                                │
│  Catalogs:      list_catalogs(), get_catalog(), register/remove_catalog │
│  Schemas:       list_schemas(), get_schema(), register/remove_schema    │
│  Tables:        list_tables(), get_table(), get_table_stats()           │
│  Partitions:    list_partitions(), list_partitions_in_range()           │
│  Files:         list_files(), register_file(), remove_file()            │
│  Utilities:     catalog_exists(), schema_exists(), table_exists()       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ implements
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      MemoryMetadataManager                               │
├─────────────────────────────────────────────────────────────────────────┤
│  Storage:                                                                │
│    • catalogs: HashMap<String, CatalogInfo>                             │
│    • schemas: HashMap<(catalog, schema), SchemaInfo>                    │
│    • tables: HashMap<(catalog, schema, table), TableInfo>               │
│    • partitions: HashMap<(catalog, schema, table, key), PartitionInfo>  │
│    • files: HashMap<path, FileInfo>                                     │
│    • partition_files: HashMap<(c,s,t,p), Vec<path>>  (reverse index)    │
│                                                                          │
│  Persistence:                                                            │
│    • JSON snapshot to disk                                               │
│    • Atomic save with temp file + rename                                 │
│    • Background auto-save (configurable interval)                        │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Model

```
CatalogInfo
├── name: String
├── created_at: i64 (Unix millis)
└── properties: HashMap<String, String>

SchemaInfo
├── catalog: String
├── name: String
├── created_at: i64
└── properties: HashMap<String, String>

TableInfo
├── catalog: String
├── schema: String
├── name: String
├── created_at: i64
├── columns: Vec<ColumnInfo>
├── partition_columns: Vec<String>
├── properties: HashMap<String, String>
└── stats: Option<TableStats>

PartitionInfo
├── catalog: String
├── schema: String
├── table: String
├── partition_key: String (e.g., "date=2024-01-15")
├── partition_values: HashMap<String, String>
├── file_count: usize
├── total_bytes: u64
├── total_rows: u64
├── min_timestamp: Option<i64>
├── max_timestamp: Option<i64>
└── created_at: i64

FileInfo
├── path: String (full path)
├── catalog: String
├── schema: String
├── table: String
├── partition_key: String
├── size_bytes: u64
├── row_count: u64
├── row_group_count: usize
├── compaction_level: u8
├── min_timestamp: Option<i64>
├── max_timestamp: Option<i64>
├── created_at: i64
├── column_stats: HashMap<String, ColumnStats>
└── etag: Option<String>
```

### Usage Example

```rust
use crate::query::catalog::{MemoryMetadataManager, MetadataManager};

// Create manager
let manager = MemoryMetadataManager::new("/data/mynadb/metadata");

// Load existing metadata
manager.load().await?;

// Or rescan from storage
manager.rescan("/data/mynadb/data").await?;

// Query metadata (fast, no I/O)
let catalogs = manager.list_catalogs().await?;
let tables = manager.list_tables("mydb", "default").await?;
let partitions = manager.list_partitions_in_range(
    "mydb", "default", "cpu",
    Some("2024-01-01"), Some("2024-01-31")
).await?;

// Register new file
manager.register_file(FileInfo {
    path: "mydb/default/cpu/date=2024-01-15/L0_abc123.parquet".to_string(),
    catalog: "mydb".to_string(),
    schema: "default".to_string(),
    table: "cpu".to_string(),
    partition_key: "date=2024-01-15".to_string(),
    size_bytes: 1024 * 1024,
    row_count: 10000,
    // ...
}).await?;

// Persist to disk
manager.save().await?;
```

### Persistence Format

The metadata is persisted as a JSON snapshot:

```json
{
  "version": 1,
  "saved_at": 1705334400000,
  "catalogs": [
    { "name": "mydb", "created_at": 1705000000000, "properties": {} }
  ],
  "schemas": [
    { "catalog": "mydb", "name": "default", "created_at": 1705000000000, "properties": {} }
  ],
  "tables": [
    { "catalog": "mydb", "schema": "default", "name": "cpu", ... }
  ],
  "partitions": [...],
  "files": [...]
}
```

### Thread Safety

- Uses `parking_lot::RwLock` for concurrent read access
- Write operations acquire exclusive lock
- Atomic save (write to temp file, then rename)

---

## 2. FileCache

### Purpose

The FileCache reduces Object Store latency by caching Parquet files on local SSD. This is especially important for:

- Repeated queries over the same time range
- Interactive/exploratory queries
- Reducing S3/MinIO bandwidth costs

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           FileCache (Trait)                              │
├─────────────────────────────────────────────────────────────────────────┤
│  Core:          get(), get_if_cached(), put(), contains(), invalidate() │
│  Batch:         get_many(), prefetch()                                  │
│  Stats:         stats(), get_entry_meta(), list_entries()               │
│  Maintenance:   evict(), save_index(), load_index(), verify()           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ implements
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         DiskFileCache                                    │
├─────────────────────────────────────────────────────────────────────────┤
│  Configuration:                                                          │
│    • max_size_bytes: 100 GB (default)                                   │
│    • cache_dir: /var/cache/mynadb/parquet                               │
│    • auto_save_interval_secs: 300 (5 min)                               │
│                                                                          │
│  Storage:                                                                │
│    • Local files: {hash}.parquet                                        │
│    • Index: cache_index.json                                            │
│                                                                          │
│  State (in-memory):                                                      │
│    • entries: HashMap<source_path, CacheEntry>                          │
│    • total_size: u64                                                    │
│    • stats: CacheStats (hits, misses, evictions, bytes)                 │
└─────────────────────────────────────────────────────────────────────────┘
```

### Cache Flow

```
                    ┌──────────────────┐
                    │   get(path)      │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  In cache index? │
                    └────────┬─────────┘
                             │
              ┌──────────────┴──────────────┐
              │ Yes                         │ No
              ▼                             ▼
    ┌──────────────────┐         ┌──────────────────┐
    │ Read local file  │         │ Fetch from       │
    │ Update access    │         │ Object Store     │
    │ Return bytes     │         └────────┬─────────┘
    └──────────────────┘                  │
                                          ▼
                                ┌──────────────────┐
                                │ Need eviction?   │
                                └────────┬─────────┘
                                         │
                          ┌──────────────┴──────────────┐
                          │ Yes                         │ No
                          ▼                             │
                ┌──────────────────┐                    │
                │ Evict lowest     │                    │
                │ priority entries │                    │
                └────────┬─────────┘                    │
                         │                             │
                         └──────────────┬──────────────┘
                                        ▼
                              ┌──────────────────┐
                              │ Write to local   │
                              │ Update index     │
                              │ Return bytes     │
                              └──────────────────┘
```

### Cache Key Design

Source paths are hashed to generate cache filenames:

```rust
pub struct CacheKey {
    pub path: String,           // e.g., "s3://bucket/db/schema/table/file.parquet"
    pub row_group: Option<usize>, // Optional: for row-group level caching
}

// Generates: "a1b2c3d4e5f6g7h8.parquet" or "a1b2c3d4e5f6g7h8_rg0.parquet"
fn to_cache_filename(&self) -> String;
```

### Eviction Strategy

The cache uses a priority-based eviction strategy that considers:

1. **Recency** - When was the file last accessed?
2. **Frequency** - How often is the file accessed?
3. **Partition Date** - Recent partitions are more valuable

```rust
fn priority_score(&self) -> f64 {
    // Recency score (higher = more recent)
    let recency_score = 1.0 / (1.0 + age_secs / 3600.0);

    // Frequency score (higher = more accesses)
    let frequency_score = (access_count as f64).ln().max(1.0);

    // Partition date bonus
    let partition_score = match days_ago {
        0 => 10.0,      // Today
        1 => 8.0,       // Yesterday
        2..=7 => 5.0,   // This week
        8..=30 => 2.0,  // This month
        _ => 1.0,       // Older
    };

    recency_score * frequency_score * partition_score
}
```

Lower priority scores are evicted first.

### Data Model

```rust
pub struct CacheEntryMeta {
    pub source_path: String,      // Original path (e.g., "s3://bucket/path")
    pub cache_path: String,       // Local cache path
    pub size_bytes: u64,
    pub etag: Option<String>,     // For invalidation
    pub cached_at: i64,           // Unix millis
    pub last_accessed: i64,       // Unix millis
    pub access_count: u64,
    pub partition_date: Option<String>, // For priority scoring
}

pub struct CacheStats {
    pub total_size_bytes: u64,
    pub max_size_bytes: u64,
    pub file_count: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub bytes_read_from_cache: u64,
    pub bytes_fetched_from_source: u64,
}
```

### Usage Example

```rust
use crate::storage::file_cache::{DiskFileCache, DiskCacheConfig, FileCache};

// Create cache
let config = DiskCacheConfig {
    cache_dir: PathBuf::from("/var/cache/mynadb/parquet"),
    max_size_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
    ..Default::default()
};
let cache = DiskFileCache::new(config).await?;

// Get file (fetches from Object Store if not cached)
let data = cache.get("s3://bucket/db/table/file.parquet", &storage).await?;

// Check if cached (no fetch)
if let Some(data) = cache.get_if_cached("s3://bucket/db/table/file.parquet").await? {
    // Use cached data
}

// Prefetch files in background
cache.prefetch(vec![
    "s3://bucket/db/table/file1.parquet".to_string(),
    "s3://bucket/db/table/file2.parquet".to_string(),
], storage).await;

// Get statistics
let stats = cache.stats().await;
println!("Hit rate: {:.1}%", stats.hit_rate() * 100.0);
println!("Cache fill: {:.1}%", stats.fill_percentage());

// Invalidate on compaction
cache.invalidate("s3://bucket/db/table/old_file.parquet").await?;

// Or invalidate entire partition
cache.invalidate_prefix("s3://bucket/db/table/date=2024-01-15/").await?;
```

### Index Persistence

The cache index is persisted as JSON:

```json
{
  "version": 1,
  "indexed_at": 1705334400000,
  "entries": [
    {
      "source_path": "s3://bucket/mydb/default/cpu/date=2024-01-15/L0_abc.parquet",
      "cache_path": "/var/cache/mynadb/parquet/a1b2c3d4e5f6g7h8.parquet",
      "size_bytes": 1048576,
      "etag": "abc123",
      "cached_at": 1705334000000,
      "last_accessed": 1705334300000,
      "access_count": 5,
      "partition_date": "2024-01-15"
    }
  ]
}
```

---

## Integration Points

### QueryEngine Integration

```rust
pub struct QueryEngine {
    // Existing fields...
    metadata_manager: Arc<dyn MetadataManager>,
    file_cache: Arc<dyn FileCache>,
}

impl QueryEngine {
    // Use metadata manager for fast lookups
    async fn list_tables(&self, catalog: &str, schema: &str) -> Result<Vec<String>> {
        let tables = self.metadata_manager.list_tables(catalog, schema).await?;
        Ok(tables.into_iter().map(|t| t.name).collect())
    }

    // Use file cache when reading Parquet files
    async fn read_parquet(&self, path: &str) -> Result<RecordBatch> {
        let data = self.file_cache.get(path, &self.storage).await?;
        // Parse Parquet from bytes...
    }
}
```

### Compactor Integration

```rust
impl Compactor {
    async fn compact(&self, files: Vec<String>, output: &str) -> Result<()> {
        // After compaction, update metadata
        for file in &files {
            self.metadata_manager.remove_file(file).await?;
            self.file_cache.invalidate(file).await?;
        }

        self.metadata_manager.register_file(FileInfo {
            path: output.to_string(),
            // ...
        }).await?;
    }
}
```

### Cache Warming

```rust
// Warm cache on startup
async fn warm_cache(
    metadata: &dyn MetadataManager,
    cache: &dyn FileCache,
    storage: &Arc<Storage>,
) {
    // Get recent partitions
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let partitions = metadata.list_partitions_in_range(
        "mydb", "default", "cpu",
        Some(&today), None
    ).await.unwrap();

    // Prefetch files
    for partition in partitions {
        let files = metadata.list_files(
            &partition.catalog,
            &partition.schema,
            &partition.table,
            &partition.partition_key,
        ).await.unwrap();

        let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
        cache.prefetch(paths, storage.clone()).await;
    }
}
```

---

## Configuration

### MetadataManager Configuration

```toml
[metadata]
# Directory for metadata snapshots
metadata_dir = "/data/mynadb/metadata"

# Auto-save interval (0 = disabled)
auto_save_interval_secs = 60

# Rescan on startup
rescan_on_startup = false
```

### FileCache Configuration

```toml
[file_cache]
# Enable file caching
enabled = true

# Cache directory
cache_dir = "/var/cache/mynadb/parquet"

# Maximum cache size
max_size_bytes = 107374182400  # 100 GB

# Index save interval
auto_save_interval_secs = 300

# Eviction threshold (start evicting when cache is this full)
eviction_threshold = 0.9  # 90%
```

---

## Performance Characteristics

### MetadataManager

| Operation | Time Complexity | Notes |
|-----------|-----------------|-------|
| list_catalogs | O(n) | n = number of catalogs |
| get_table | O(1) | HashMap lookup |
| list_partitions_in_range | O(n) | n = partitions in table |
| register_file | O(1) | HashMap insert |
| save | O(n) | n = total entries |
| load | O(n) | n = total entries |

### FileCache

| Operation | Time Complexity | Notes |
|-----------|-----------------|-------|
| get (hit) | O(1) + disk I/O | Index lookup + file read |
| get (miss) | O(1) + network + disk I/O | Fetch + write + index |
| contains | O(1) | Index lookup only |
| evict | O(n log n) | Sort by priority, then evict |
| save_index | O(n) | Serialize all entries |

### Memory Usage

- **MetadataManager**: ~500 bytes per file entry
- **FileCache Index**: ~300 bytes per cached file entry

For a system with 100,000 files:
- MetadataManager: ~50 MB
- FileCache Index: ~30 MB

---

## Future Enhancements

### Planned

1. **SQLite-based MetadataManager** - For larger deployments with millions of files
2. **Memory-tier FileCache** - L1 memory cache before L2 disk cache
3. **Distributed Cache** - Shared cache across multiple nodes
4. **Bloom Filters** - For faster "not in cache" checks
5. **Compression** - Compress cached files to save disk space

### Considerations

1. **Cache Coherence** - Handle concurrent writes/compaction
2. **TTL-based Invalidation** - Auto-expire cache entries
3. **Adaptive Caching** - Learn access patterns for better prefetching
4. **Tiered Storage** - NVMe > SSD > HDD cache tiers
