---
name: rust-tsdb-patterns
description: This skill should be used when the user is writing Rust code for time-series database components — memtables, write-ahead logs, compaction, ingestion parsing, cache management, or any storage engine code. Also use when discussing concurrency patterns, memory management, or performance-critical Rust code.
version: 1.0.0
---

# Rust Patterns for Time-Series Database Development

## Memory Management

### Arena / Slab Allocation
For high-throughput ingestion, avoid per-point heap allocation:
```rust
// Bad: allocation per point
let points: Vec<Point> = lines.iter().map(|l| parse(l)).collect();

// Better: pre-allocate with capacity estimate
let mut points = Vec::with_capacity(lines.len());
for line in lines { points.push(parse(line)); }

// Best: parse into pre-allocated Arrow RecordBatch
let mut builder = StringBuilder::with_capacity(lines.len(), avg_len * lines.len());
```

### String Interning for Tags
Tags repeat heavily in time-series data. Intern them:
```rust
use std::collections::HashMap;
use std::sync::Arc;

struct TagInterner {
    map: HashMap<String, Arc<str>>,
}

impl TagInterner {
    fn intern(&mut self, s: &str) -> Arc<str> {
        if let Some(existing) = self.map.get(s) {
            existing.clone()
        } else {
            let interned: Arc<str> = Arc::from(s);
            self.map.insert(s.to_string(), interned.clone());
            interned
        }
    }
}
```

### Zero-Copy Parsing
For line protocol parsing, avoid String allocation:
```rust
// Bad: allocate String for each tag
fn parse_tag(input: &str) -> (String, String) {
    let (k, v) = input.split_once('=').unwrap();
    (k.to_string(), v.to_string())
}

// Better: return &str references into input
fn parse_tag(input: &str) -> (&str, &str) {
    input.split_once('=').unwrap()
}
```

## Concurrency Patterns

### Sharded Write Cache
MynaDB uses 32 shards to reduce lock contention:
```rust
const NUM_SHARDS: usize = 32;

struct ShardedCache {
    shards: Vec<RwLock<HashMap<PartitionKey, Vec<Point>>>>,
}

impl ShardedCache {
    fn shard_for(&self, key: &PartitionKey) -> &RwLock<...> {
        let idx = hash(key) % NUM_SHARDS;
        &self.shards[idx]
    }

    fn insert(&self, key: PartitionKey, point: Point) {
        let shard = self.shard_for(&key);
        let mut guard = shard.write().unwrap();
        guard.entry(key).or_default().push(point);
    }
}
```

### Background Job Pattern
```rust
async fn start_background_job(
    interval: Duration,
    shutdown: CancellationToken,
    work: impl Fn() -> Pin<Box<dyn Future<Output = Result<()>>>> + Send + 'static,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = work().await {
                        error!("Background job error: {}", e);
                    }
                }
                _ = shutdown.cancelled() => break,
            }
        }
    })
}
```

### Concurrent Compaction with Semaphore
```rust
let semaphore = Arc::new(Semaphore::new(max_concurrent));

for partition in partitions {
    let permit = semaphore.clone().acquire_owned().await?;
    tokio::spawn(async move {
        compact_partition(partition).await;
        drop(permit); // release semaphore
    });
}
```

## WAL Patterns

### Entry Format
```
[timestamp: u64][type: u8][db_len: u16][db: bytes][payload_len: u32][payload: bytes][crc32: u32]
```

### Rotation Strategy
```rust
impl Wal {
    async fn maybe_rotate(&mut self) -> Result<()> {
        if self.current_size > self.max_size {
            self.current_file = self.create_new_segment().await?;
            self.current_size = 0;
        }
        Ok(())
    }
}
```

### Recovery
```rust
async fn recover(wal_dir: &Path) -> Result<Vec<WalEntry>> {
    let mut entries = Vec::new();
    for segment in list_segments(wal_dir).await? {
        let reader = BufReader::new(File::open(segment).await?);
        loop {
            match read_entry(&mut reader).await {
                Ok(entry) => {
                    if verify_crc(&entry) {
                        entries.push(entry);
                    } else {
                        warn!("CRC mismatch, truncating at this point");
                        break;
                    }
                }
                Err(_) => break, // EOF or corruption
            }
        }
    }
    entries
}
```

## Error Handling Patterns

### Domain Errors
```rust
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Partition not found: {catalog}.{schema}.{table}/{date}")]
    PartitionNotFound { catalog: String, schema: String, table: String, date: String },

    #[error("Write conflict on {path}")]
    WriteConflict { path: String },

    #[error("Storage backend error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}
```

### Graceful Degradation
```rust
// Don't fail writes if metadata update fails
match metadata_manager.register_file(file_info).await {
    Ok(_) => {},
    Err(e) => {
        warn!("Failed to register file in metadata, will be discovered on next scan: {}", e);
        // File exists on disk, will be found by discovery
    }
}
```

## Performance-Critical Code

### Batch Processing
Always process data in batches, not row-by-row:
```rust
// Bad: process one row at a time
for row in input {
    output.push(transform(row));
}

// Good: process in Arrow columnar batches
let input_array = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
let output: Float64Array = input_array.iter()
    .map(|v| v.map(|x| x * 2.0))
    .collect();
```

### SIMD-Friendly Loops
Arrow arrays are contiguous memory. Keep processing sequential:
```rust
let values = array.values(); // &[f64] contiguous slice
let mut sum = 0.0;
for &v in values {
    sum += v; // CPU can auto-vectorize this
}
```

## Code Standards (from CLAUDE.md)

- **No hard-coded strings**: Use constants for paths, tag names, extensions
- **Use PathBuf**: `Path::new().join()` instead of `format!()` for paths
- **Single source of truth**: Constants at module top or constants module
- **Helper methods**: Extract repeated path construction
