# Write Performance Optimization Plans

Current throughput: ~145K rows/s (tsbs benchmark, sync_on_write=false)
Target: 500K+ rows/s

## Current Write Path

```
HTTP POST /write/:db
    → Axum body extraction (copy)
    → WAL async write (spawn overhead)
    → parse_multiple_lines() (string allocations)
    → cache.add_points() (mutex lock per shard)
    → Response
```

## Optimization Plans

### 1. Reduce String Allocations in Parsing (High Impact)

**Problem:** Every `parse_line_protocol` call creates new `String` for measurement, tags, fields.

**Current code:**
```rust
let measurement = parts[0].to_string();  // allocation
tags.insert(key.to_string(), value.to_string());  // 2 allocations per tag
fields.insert(key.to_string(), field_value);  // allocation per field
```

**Solution:** Use `&str` slices during parsing, only allocate when storing in cache.

**Implementation:**
- Create `PointRef<'a>` struct with borrowed strings
- Only convert to owned `Point` when inserting into cache
- Use `Cow<'a, str>` for conditional ownership

**Expected Impact:** ~20-30% improvement in parsing throughput

---

### 2. Batch HTTP Body Processing (High Impact)

**Problem:** Parse entire body into Vec<Point>, then process all points. Large intermediate allocations.

**Current code:**
```rust
let points = parse_multiple_lines(&data, &schema_config);  // allocates all points
cache.add_points(&db, points);  // processes all at once
```

**Solution:** Stream parsing - parse and insert in chunks of 1000 points.

**Implementation:**
```rust
for chunk in data.lines().chunks(1000) {
    let points: Vec<Point> = chunk.filter_map(|line| parse_line_protocol(line, &config).ok()).collect();
    cache.add_points(&db, points);
}
```

**Expected Impact:** ~10-15% improvement, reduced memory pressure

---

### 3. String Interning for Tag/Field Keys (Medium-High Impact)

**Problem:** Every point allocates new strings for common keys like "host", "service", "region".

**Current:**
- 10 tags × 100K points = 1M string allocations just for tag keys
- Most are duplicates ("host", "service", etc.)

**Solution:** Intern common strings, reuse across points.

**Implementation Options:**

Option A: Global string interner
```rust
use string_interner::{StringInterner, DefaultSymbol};

lazy_static! {
    static ref INTERNER: Mutex<StringInterner> = Mutex::new(StringInterner::default());
}
```

Option B: Use `Arc<str>` with a cache
```rust
struct StringCache {
    cache: DashMap<String, Arc<str>>,
}

impl StringCache {
    fn intern(&self, s: &str) -> Arc<str> {
        if let Some(existing) = self.cache.get(s) {
            return existing.clone();
        }
        let arc: Arc<str> = s.into();
        self.cache.insert(s.to_string(), arc.clone());
        arc
    }
}
```

**Expected Impact:** ~15-25% reduction in allocations

---

### 4. Pre-allocated Point Buffers (Medium Impact)

**Problem:** Creating new `Point` structs and HashMaps per line.

**Current:**
```rust
let mut tags = HashMap::with_capacity(tag_count);  // allocation
let mut fields = HashMap::with_capacity(parts.len());  // allocation
```

**Solution:** Object pool for Points, reuse HashMap capacity.

**Implementation:**
```rust
struct PointPool {
    points: Vec<Point>,
}

impl PointPool {
    fn acquire(&mut self) -> Point {
        self.points.pop().unwrap_or_else(|| Point::new())
    }

    fn release(&mut self, mut point: Point) {
        point.tags.clear();
        point.fields.clear();
        self.points.push(point);
    }
}
```

**Expected Impact:** ~10-15% improvement

---

### 5. SIMD-accelerated Line Protocol Parsing (Medium Impact)

**Problem:** Character-by-character parsing is slow.

**Current:**
```rust
let parts: Vec<&str> = line.split_whitespace().collect();
let parts: Vec<&str> = part.split(',').collect();
```

**Solution:** Use `memchr` crate for vectorized delimiter search.

**Implementation:**
```rust
use memchr::{memchr, memchr2, memchr3};

// Find space (field separator)
if let Some(pos) = memchr(b' ', line.as_bytes()) {
    let measurement_tags = &line[..pos];
    let rest = &line[pos+1..];
}

// Find comma or equals
if let Some(pos) = memchr2(b',', b'=', part.as_bytes()) {
    // ...
}
```

**Expected Impact:** ~10-20% improvement in parsing

**Dependency:** Add `memchr = "2.7"` to Cargo.toml

---

### 6. Reduce Lock Granularity (Medium Impact)

**Problem:** Lock per shard for entire insert operation.

**Current:**
```rust
let mut shard = self.shards[shard_idx].data.lock();
shard.entry(key).or_insert_with(Vec::new).extend(points);  // lock held during extend
```

**Solution:** Lock-free append to pre-allocated buffers, batch commits.

**Implementation:**
```rust
// Use crossbeam's SegQueue for lock-free append
struct CacheShard {
    pending: SegQueue<(TableKey, Vec<Point>)>,
    data: Mutex<HashMap<TableKey, Vec<Point>>>,
}

// Writers append to lock-free queue
shard.pending.push((key, points));

// Background task periodically drains queue under lock
fn drain_pending(&self) {
    let mut data = self.data.lock();
    while let Some((key, points)) = self.pending.pop() {
        data.entry(key).or_default().extend(points);
    }
}
```

**Expected Impact:** ~15-20% improvement under high concurrency

**Dependency:** Add `crossbeam = "0.8"` to Cargo.toml

---

### 7. Async Channel for Cache Writes (Medium Impact)

**Problem:** Direct mutex lock on cache shard blocks caller.

**Solution:** Use `tokio::sync::mpsc` channel, dedicated writer task per shard.

**Implementation:**
```rust
struct CacheShard {
    sender: mpsc::Sender<CacheCommand>,
}

enum CacheCommand {
    Insert { key: TableKey, points: Vec<Point> },
    Flush { response: oneshot::Sender<()> },
}

// Writer task per shard
async fn shard_writer(mut rx: mpsc::Receiver<CacheCommand>, data: Arc<Mutex<Data>>) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            CacheCommand::Insert { key, points } => {
                let mut data = data.lock();
                data.entry(key).or_default().extend(points);
            }
            // ...
        }
    }
}
```

**Expected Impact:** ~10-15% improvement, non-blocking inserts

---

### 8. Disable WAL for Benchmarks (Quick Win)

**Problem:** Even async WAL has spawn overhead per request.

**Current:**
```rust
if let Some(ref wal) = state.wal {
    let wal = wal.clone();
    tokio::spawn(async move {  // spawn overhead
        let _ = wal.log_data_write(&db, &data).await;
    });
}
```

**Solution:** Add `wal.enabled=false` config option to completely skip WAL.

**Implementation:**
```toml
[wal]
enabled = false  # Skip WAL entirely for benchmarks
```

```rust
if state.wal_enabled {
    if let Some(ref wal) = state.wal {
        // ... WAL logic
    }
}
```

**Expected Impact:** ~5-10% improvement (removes spawn overhead)

---

### 9. HTTP Body Zero-Copy (Low-Medium Impact)

**Problem:** Axum extracts body as String, causing copy.

**Current:**
```rust
async fn write_data(body: String) -> impl IntoResponse {
    // body is already copied from network buffer
}
```

**Solution:** Use `bytes::Bytes` to avoid copying request body.

**Implementation:**
```rust
use axum::body::Bytes;

async fn write_data(body: Bytes) -> impl IntoResponse {
    // body is zero-copy reference to network buffer
    let data = std::str::from_utf8(&body).map_err(|_| ...)?;
    // ...
}
```

**Expected Impact:** ~5% improvement for large payloads

---

### 10. Parallel Line Parsing with Rayon (Medium Impact)

**Problem:** Single-threaded parsing doesn't utilize all CPU cores.

**Solution:** Split input into chunks, parse in parallel using rayon.

**Implementation:**
```rust
use rayon::prelude::*;

fn parse_multiple_lines_parallel(data: &str, schema_config: &SchemaConfig) -> Vec<Point> {
    let lines: Vec<&str> = data.lines().collect();

    lines.par_chunks(1000)
        .flat_map(|chunk| {
            chunk.iter()
                .filter_map(|line| parse_line_protocol(line, schema_config).ok())
                .collect::<Vec<_>>()
        })
        .collect()
}
```

**Expected Impact:** ~2-4x improvement on multi-core systems

**Dependency:** Add `rayon = "1.10"` to Cargo.toml

**Considerations:**
- Only beneficial for large batches (>10K lines)
- Thread pool overhead for small batches
- Need to benchmark crossover point

---

## Implementation Priority

| Priority | Optimization | Impact | Effort | Dependencies |
|----------|-------------|--------|--------|--------------|
| 1 | #8 Disable WAL | Quick test | Low | None |
| 2 | #1 Reduce allocations | High | Medium | None |
| 3 | #5 SIMD parsing | Medium | Low | memchr |
| 4 | #3 String interning | Medium-High | Medium | dashmap or string-interner |
| 5 | #10 Parallel parsing | Medium | Low | rayon |
| 6 | #6 Lock-free append | Medium | Medium | crossbeam |
| 7 | #9 Zero-copy body | Low-Medium | Low | None |
| 8 | #4 Point pools | Medium | Medium | None |
| 9 | #2 Chunked processing | Medium | Low | None |
| 10 | #7 Async channels | Medium | High | None |

---

## Benchmarking Commands

```bash
# Run tsbs benchmark
tsbs_load_influx --file=data.gz --workers=8 --urls=http://localhost:7000

# Profile with perf
perf record -g ./target/release/tsdb-stats
perf report

# Flamegraph
cargo flamegraph --bin tsdb-stats

# Memory profiling
heaptrack ./target/release/tsdb-stats
```

---

## Target Architecture

After optimizations:

```
HTTP POST /write/:db
    → Bytes extraction (zero-copy)
    → [Optional] Async WAL channel
    → Parallel parsing (rayon)
        → SIMD delimiter search (memchr)
        → String interning (common keys)
        → Pre-allocated Points
    → Lock-free cache append (crossbeam)
    → Background batch commit
    → Response (immediate)
```

Target: 500K-1M rows/s
