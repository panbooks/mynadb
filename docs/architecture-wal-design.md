# Write-Ahead Log (WAL) Design

## Overview

Add a Write-Ahead Log to ensure data durability for write operations and series tracking. The WAL will prevent data loss in case of crashes by recording operations before they're applied to in-memory structures.

## Architecture

### WAL Scope

The WAL will cover two critical write paths:

1. **Data Writes** (`/write/:db` endpoint)
   - Log line protocol data before adding to DataCache
   - Replay on startup to recover unflushed writes

2. **Series Tracking** (series_tracker)
   - Log new series before updating in-memory cache
   - Replay on startup to recover series metadata

### Storage Structure

```
data/
├── _wal/                      # WAL directory
│   ├── data_YYYYMMDD_HHMMSS.wal      # Data write logs
│   └── series_YYYYMMDD_HHMMSS.wal    # Series tracking logs
└── {database}/                # Existing data structure
```

### WAL File Format

Use a simple binary format with length-prefixed entries:

```
[Entry]
- timestamp: u64 (8 bytes) - nanoseconds since epoch
- entry_type: u8 (1 byte) - 0=data_write, 1=series_track
- db_len: u32 (4 bytes)
- db_name: [u8; db_len]
- payload_len: u32 (4 bytes)
- payload: [u8; payload_len] - serialized data (bincode or similar)
- checksum: u32 (4 bytes) - CRC32 of entry
```

## Implementation Plan

### Phase 1: Core WAL Module

**File: `src/wal.rs`**

```rust
pub struct Wal {
    wal_dir: String,
    data_writer: Arc<Mutex<WalWriter>>,
    series_writer: Arc<Mutex<WalWriter>>,
}

impl Wal {
    pub fn new(wal_dir: &str) -> Result<Self, String>;

    // Write operations
    pub async fn log_data_write(&self, db: &str, line_protocol: &str) -> Result<(), String>;
    pub async fn log_series_track(&self, db: &str, records: &[SeriesRecord]) -> Result<(), String>;

    // Recovery
    pub async fn replay_data_writes<F>(&self, callback: F) -> Result<usize, String>
        where F: Fn(&str, &str) -> Result<(), String>;

    pub async fn replay_series_tracks<F>(&self, callback: F) -> Result<usize, String>
        where F: Fn(&str, &[SeriesRecord]) -> Result<(), String>;

    // Maintenance
    pub async fn rotate(&self) -> Result<(), String>;
    pub async fn cleanup_old_files(&self, older_than_secs: u64) -> Result<usize, String>;
}

struct WalWriter {
    file: File,
    path: PathBuf,
    bytes_written: u64,
}
```

### Phase 2: Integration Points

#### 2.1 Data Write Path

**In `main.rs` write endpoint:**

```rust
async fn write_handler(
    State(state): State<Arc<AppState>>,
    Path(db): Path<String>,
    data: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // STEP 1: Write to WAL first (durability)
    if let Err(e) = state.wal.log_data_write(&db, &data).await {
        error!("WAL write failed: {}", e);
        return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("WAL error: {}", e)));
    }

    // STEP 2: Parse and process (existing logic)
    let points = parse_multiple_lines(&data, &state.config.schema);

    // STEP 3: Track series
    state.series_tracker.track_points(&db, &points).await;

    // STEP 4: Add to cache
    // ... existing cache logic ...

    Ok((StatusCode::NO_CONTENT, ""))
}
```

#### 2.2 Series Tracker Integration

**In `series_tracker.rs`:**

```rust
pub async fn track_points(&self, db: &str, points: &[Point]) -> Result<(), String> {
    // Existing logic to identify new series...

    if !new_records.is_empty() {
        // STEP 1: Write to WAL before updating cache
        self.wal.log_series_track(db, &new_records).await?;

        // STEP 2: Update in-memory cache (existing logic)
        // ...
    }

    Ok(())
}
```

### Phase 3: Recovery on Startup

**In `main.rs` startup:**

```rust
#[tokio::main]
async fn main() {
    // ... config loading ...

    // Initialize WAL
    let wal = Arc::new(Wal::new(&format!("{}/{}", config.storage.data_dir, "_wal"))
        .expect("Failed to initialize WAL"));

    // RECOVERY PHASE
    info!("Starting WAL recovery...");

    // Recover data writes
    let recovered_data = wal.replay_data_writes(|db, line_protocol| {
        // Re-process each logged write
        let points = parse_multiple_lines(line_protocol, &config.schema);
        // Add to cache or directly write to parquet
        Ok(())
    }).await?;

    info!("Recovered {} data write entries from WAL", recovered_data);

    // Recover series tracking
    let recovered_series = wal.replay_series_tracks(|db, records| {
        // Re-apply series tracking
        series_tracker.apply_records(db, records).await
    }).await?;

    info!("Recovered {} series tracking entries from WAL", recovered_series);

    // Initialize components...
}
```

### Phase 4: WAL Cleanup

**After successful flush:**

```rust
impl DataCache {
    pub async fn flush(&self) -> Result<usize, String> {
        // ... existing flush logic ...

        // After successful flush, WAL entries can be removed
        if let Some(wal) = &self.wal {
            wal.rotate().await?;
        }

        Ok(files_written)
    }
}
```

**Background cleanup task:**

```rust
// In main.rs
tokio::spawn({
    let wal = Arc::clone(&wal);
    async move {
        let mut interval = interval(Duration::from_secs(3600)); // hourly
        loop {
            interval.tick().await;

            // Clean up WAL files older than 24 hours (already flushed)
            match wal.cleanup_old_files(24 * 3600).await {
                Ok(count) => info!("Cleaned up {} old WAL files", count),
                Err(e) => error!("WAL cleanup error: {}", e),
            }
        }
    }
});
```

## Configuration

**Add to `config.toml`:**

```toml
[wal]
enabled = true
sync_on_write = true    # fsync after each write (slower but safer)
rotate_size_mb = 64     # Rotate when file exceeds this size
cleanup_interval_secs = 3600
retention_hours = 24    # Keep WAL files for 24 hours after creation
```

**Add to `config.rs`:**

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct WalConfig {
    #[serde(default = "default_wal_enabled")]
    pub enabled: bool,

    #[serde(default = "default_sync_on_write")]
    pub sync_on_write: bool,

    #[serde(default = "default_rotate_size_mb")]
    pub rotate_size_mb: u64,

    #[serde(default = "default_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,

    #[serde(default = "default_retention_hours")]
    pub retention_hours: u64,
}
```

## Performance Considerations

### Write Amplification
- Each write is written twice: once to WAL, once to cache
- Mitigation: Batch WAL writes, use buffered I/O

### Sync Strategy
- **sync_on_write=true**: Durability guaranteed, but slower (~1-5ms per write)
- **sync_on_write=false**: Faster, but may lose last few seconds of data on crash

### WAL File Size
- Rotate at 64MB by default to prevent unbounded growth
- Cleanup old files after data is safely flushed to Parquet

## Recovery Guarantees

1. **Data Writes**: All writes logged to WAL will be recovered and re-processed
2. **Series Tracking**: All new series will be re-tracked
3. **Order Preservation**: Entries are replayed in the order they were written
4. **Crash Safety**: WAL is synced to disk before returning success (if sync_on_write=true)

## Implementation Order

1. ✅ Create WAL module structure
2. ✅ Implement binary format encoding/decoding
3. ✅ Implement WAL append operations
4. ✅ Implement WAL replay logic
5. ✅ Add configuration support
6. ✅ Integrate into write endpoint
7. ✅ Integrate into series tracker
8. ✅ Add recovery on startup
9. ✅ Add cleanup logic
10. ✅ Test crash recovery scenarios

## Testing Strategy

### Unit Tests
- WAL encoding/decoding
- Checksum validation
- File rotation logic

### Integration Tests
1. Write data → crash simulator → restart → verify recovery
2. High-volume writes → verify no data loss
3. Concurrent writes → verify no corruption

### Performance Tests
- Measure write latency with/without WAL
- Measure recovery time with various WAL sizes
- Benchmark sync vs async modes

## Alternative: Use Existing WAL Library

Instead of implementing from scratch, consider using:
- **redb**: Embedded database with built-in WAL
- **sled**: Embedded database with crash recovery
- **rocksdb**: Key-value store with WAL support

Trade-offs:
- ✅ Battle-tested, production-ready
- ✅ Less code to maintain
- ❌ Additional dependency
- ❌ Less control over format

## Next Steps

Should we:
1. **Implement custom WAL** as outlined above (more control, simpler)
2. **Use existing library** like redb/sled (more robust, battle-tested)

Which approach do you prefer?
