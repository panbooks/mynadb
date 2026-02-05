# Proposal: Stats Service Cluster Edition (Shared-Storage Architecture)

**Document Version:** 1.0
**Date:** 2026-01-13
**Status:** Draft
**Author:** Stats Service Team

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Component Design](#3-component-design)
4. [Data Flow](#4-data-flow)
5. [Coordination Layer](#5-coordination-layer)
6. [Configuration](#6-configuration)
7. [Implementation Plan](#7-implementation-plan)
8. [Comparison with Shared-Nothing](#8-comparison-with-shared-nothing)
9. [Operational Guide](#9-operational-guide)

---

## 1. Executive Summary

This proposal describes a **Shared-Storage Architecture** for Stats Service Cluster Edition. Unlike the shared-nothing approach, this architecture separates compute from storage, using object storage (S3, MinIO, or shared filesystem) as the single source of truth.

### Key Benefits

| Benefit | Description |
|---------|-------------|
| **Simpler Operations** | No data rebalancing, no shard management |
| **Elastic Compute** | Add/remove nodes without data migration |
| **Cost Efficient** | Leverage cheap object storage |
| **Simpler Recovery** | No complex replication protocols |
| **Cloud Native** | Natural fit for Kubernetes and cloud environments |

### Architecture at a Glance

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENTS                                   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                     LOAD BALANCER                                │
└───────────────────────────┬─────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
   ┌─────────┐         ┌─────────┐         ┌─────────┐
   │ Node 1  │         │ Node 2  │         │ Node 3  │
   │ Compute │         │ Compute │         │ Compute │
   │ + Cache │         │ + Cache │         │ + Cache │
   └────┬────┘         └────┬────┘         └────┬────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                     SHARED STORAGE                               │
│                   (S3 / MinIO / NFS)                            │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  catalog_1/  │  │  catalog_2/  │  │  _metadata/  │          │
│  │  ├─ schema/  │  │  ├─ schema/  │  │  ├─ locks/   │          │
│  │  │  └─ table/│  │  │  └─ table/│  │  └─ catalog/ │          │
│  │  │    └─*.pq │  │  │    └─*.pq │  │              │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                  COORDINATION SERVICE                            │
│                      (etcd / Redis)                              │
│  • Write locks    • Leader election    • Cache invalidation     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Architecture Overview

### 2.1 Design Principles

1. **Stateless Compute Nodes**: Nodes can be added/removed freely
2. **Storage as Source of Truth**: All durable data in object storage
3. **Local Caching**: Each node caches hot data for performance
4. **Distributed Coordination**: Lightweight coordination for writes and cache

### 2.2 Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **Compute Node** | Query execution, write buffering, local caching |
| **Shared Storage** | Durable parquet files, WAL segments |
| **Coordinator** | Write locks, leader election, cache invalidation |
| **Load Balancer** | Request distribution, health checks |

### 2.3 Node Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        COMPUTE NODE                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                     HTTP / Flight API                     │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│              ┌───────────────┼───────────────┐                  │
│              │               │               │                  │
│              ▼               ▼               ▼                  │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐       │
│  │ Write Handler │  │ Query Engine  │  │ Cache Manager │       │
│  │               │  │  (DataFusion) │  │               │       │
│  └───────┬───────┘  └───────┬───────┘  └───────┬───────┘       │
│          │                  │                  │                │
│          ▼                  │                  ▼                │
│  ┌───────────────┐          │          ┌───────────────┐       │
│  │  Write Buffer │          │          │  Local Cache  │       │
│  │  (In-Memory)  │          │          │  (LRU Disk)   │       │
│  └───────┬───────┘          │          └───────┬───────┘       │
│          │                  │                  │                │
│          └──────────────────┼──────────────────┘                │
│                             │                                   │
│                             ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                   Storage Client                          │   │
│  │              (S3 SDK / MinIO Client)                      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Design

### 3.1 Shared Storage Layer

**Storage Layout:**
```
s3://stats-bucket/
├── data/
│   └── {catalog}/
│       └── {schema}/
│           └── {table}/
│               └── date={YYYY-MM-DD}/
│                   ├── L0_xxx.parquet      # Recent writes
│                   ├── L1_xxx.parquet      # Compacted
│                   └── L2_xxx.parquet      # Fully compacted
├── wal/
│   └── {catalog}/
│       └── {node_id}/
│           ├── segment_00001.wal
│           ├── segment_00002.wal
│           └── segment_00003.wal
├── metadata/
│   ├── catalogs.json                       # Catalog registry
│   ├── tables/
│   │   └── {catalog}_{schema}_{table}.json # Table metadata
│   └── compaction/
│       └── {catalog}_{schema}_{table}.lock # Compaction locks
└── checkpoints/
    └── {catalog}/
        └── checkpoint_xxx.json             # Query checkpoints
```

**Storage Interface:**
```rust
#[async_trait]
pub trait ObjectStorage: Send + Sync {
    /// List objects with prefix
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StorageError>;

    /// Get object content
    async fn get(&self, path: &str) -> Result<Bytes, StorageError>;

    /// Put object content
    async fn put(&self, path: &str, data: Bytes) -> Result<(), StorageError>;

    /// Delete object
    async fn delete(&self, path: &str) -> Result<(), StorageError>;

    /// Check if object exists
    async fn exists(&self, path: &str) -> Result<bool, StorageError>;

    /// Get object metadata
    async fn head(&self, path: &str) -> Result<ObjectMeta, StorageError>;
}
```

**Supported Backends:**

| Backend | Use Case | Configuration |
|---------|----------|---------------|
| **S3** | AWS production | `s3://bucket/prefix` |
| **MinIO** | Self-hosted, on-prem | `s3://bucket` with endpoint override |
| **GCS** | Google Cloud | `gs://bucket/prefix` |
| **Azure Blob** | Azure Cloud | `az://container/prefix` |
| **Local FS** | Development/testing | `file:///path/to/data` |
| **NFS** | Shared filesystem | `file:///mnt/nfs/data` |

### 3.2 Write Buffer

Each node maintains a local write buffer before flushing to shared storage:

```rust
pub struct WriteBuffer {
    /// In-memory buffer per table
    buffers: HashMap<TablePath, TableBuffer>,
    /// Maximum buffer size before flush
    max_buffer_size: usize,
    /// Maximum time before flush
    flush_interval: Duration,
    /// Local WAL for durability
    local_wal: LocalWal,
}

pub struct TableBuffer {
    /// Buffered record batches
    batches: Vec<RecordBatch>,
    /// Total bytes in buffer
    size_bytes: usize,
    /// First write timestamp
    first_write: Instant,
}
```

**Write Buffer Flow:**
```
Write Request
     │
     ▼
┌─────────────┐
│ Local WAL   │  ← Durability (optional, for crash recovery)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│Write Buffer │  ← In-memory accumulation
└──────┬──────┘
       │
       │ Flush trigger: size OR time
       ▼
┌─────────────┐
│ Convert to  │
│  Parquet    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│Upload to S3 │  ← Durable storage
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Invalidate  │  ← Notify other nodes
│   Cache     │
└─────────────┘
```

### 3.3 Local Cache

Each node maintains a local disk cache for frequently accessed parquet files:

```rust
pub struct LocalCache {
    /// Cache directory
    cache_dir: PathBuf,
    /// Maximum cache size
    max_size_bytes: u64,
    /// Current cache size
    current_size: AtomicU64,
    /// LRU tracking
    lru: Mutex<LruCache<String, CacheEntry>>,
    /// Cache hit/miss stats
    stats: CacheStats,
}

pub struct CacheEntry {
    /// Local file path
    local_path: PathBuf,
    /// Object storage path
    remote_path: String,
    /// ETag for invalidation
    etag: String,
    /// File size
    size_bytes: u64,
    /// Last access time
    last_access: Instant,
}
```

**Cache Strategy:**
```
Query for file X
       │
       ▼
┌─────────────────┐
│ Check local     │
│ cache           │
└────────┬────────┘
         │
    ┌────┴────┐
    │ Cached? │
    └────┬────┘
         │
    Yes  │  No
    ┌────┴────────────────┐
    │                     │
    ▼                     ▼
┌─────────┐       ┌─────────────┐
│ Check   │       │ Download    │
│ ETag    │       │ from S3     │
└────┬────┘       └──────┬──────┘
     │                   │
  Match?                 │
     │                   │
  Yes│ No                │
    ┌┴──────────┐        │
    │           │        │
    ▼           ▼        ▼
┌───────┐  ┌────────┐  ┌────────┐
│ Use   │  │Download│  │ Cache  │
│ Cache │  │  New   │  │ File   │
└───────┘  └────────┘  └────────┘
```

### 3.4 Query Engine

Query execution leverages DataFusion with object storage integration:

```rust
pub struct SharedStorageQueryEngine {
    /// DataFusion session context
    ctx: SessionContext,
    /// Object storage client
    storage: Arc<dyn ObjectStorage>,
    /// Local cache
    cache: Arc<LocalCache>,
    /// Table registry
    registry: Arc<TableRegistry>,
}

impl SharedStorageQueryEngine {
    pub async fn execute_query(&self, sql: &str) -> Result<RecordBatchStream> {
        // 1. Parse and plan query
        let plan = self.ctx.sql(sql).await?;

        // 2. Identify required parquet files
        let files = self.get_required_files(&plan).await?;

        // 3. Ensure files are cached locally
        for file in &files {
            self.cache.ensure_cached(file).await?;
        }

        // 4. Execute query against local cache
        let stream = plan.execute_stream().await?;

        Ok(stream)
    }
}
```

---

## 4. Data Flow

### 4.1 Write Path

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              WRITE PATH                                      │
└─────────────────────────────────────────────────────────────────────────────┘

  Client              Node                Storage              Coordinator
    │                  │                     │                      │
    │ POST /write/db   │                     │                      │
    │─────────────────►│                     │                      │
    │                  │                     │                      │
    │                  │ 1. Acquire write    │                      │
    │                  │    lock (optional)  │                      │
    │                  │─────────────────────────────────────────────►
    │                  │                     │                      │
    │                  │◄────────────────────────────────────────────
    │                  │    Lock granted     │                      │
    │                  │                     │                      │
    │                  │ 2. Write to local   │                      │
    │                  │    WAL (optional)   │                      │
    │                  │                     │                      │
    │                  │ 3. Buffer in memory │                      │
    │                  │                     │                      │
    │◄─────────────────│                     │                      │
    │    204 OK        │                     │                      │
    │                  │                     │                      │
    │                  │     ... time passes / buffer fills ...     │
    │                  │                     │                      │
    │                  │ 4. Flush trigger    │                      │
    │                  │                     │                      │
    │                  │ 5. Convert to       │                      │
    │                  │    Parquet          │                      │
    │                  │                     │                      │
    │                  │ 6. Upload to S3     │                      │
    │                  │────────────────────►│                      │
    │                  │                     │                      │
    │                  │◄────────────────────│                      │
    │                  │    Upload complete  │                      │
    │                  │                     │                      │
    │                  │ 7. Notify cache     │                      │
    │                  │    invalidation     │                      │
    │                  │─────────────────────────────────────────────►
    │                  │                     │                      │
```

### 4.2 Query Path

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              QUERY PATH                                      │
└─────────────────────────────────────────────────────────────────────────────┘

  Client              Node                Cache               Storage
    │                  │                    │                    │
    │ GET /query/db    │                    │                    │
    │ SELECT * FROM... │                    │                    │
    │─────────────────►│                    │                    │
    │                  │                    │                    │
    │                  │ 1. Parse SQL       │                    │
    │                  │ 2. List files      │                    │
    │                  │────────────────────────────────────────►│
    │                  │                    │                    │
    │                  │◄───────────────────────────────────────│
    │                  │    [file1, file2, file3]               │
    │                  │                    │                    │
    │                  │ 3. Check cache     │                    │
    │                  │───────────────────►│                    │
    │                  │                    │                    │
    │                  │◄───────────────────│                    │
    │                  │  file1: HIT        │                    │
    │                  │  file2: MISS       │                    │
    │                  │  file3: HIT        │                    │
    │                  │                    │                    │
    │                  │ 4. Download missing│                    │
    │                  │───────────────────────────────────────►│
    │                  │                    │                    │
    │                  │◄──────────────────────────────────────│
    │                  │    file2 content   │                    │
    │                  │                    │                    │
    │                  │ 5. Cache file2     │                    │
    │                  │───────────────────►│                    │
    │                  │                    │                    │
    │                  │ 6. Execute query   │                    │
    │                  │    (DataFusion)    │                    │
    │                  │                    │                    │
    │◄─────────────────│                    │                    │
    │    Results       │                    │                    │
```

### 4.3 Compaction

Compaction runs on a designated node (leader) or any node with distributed locking:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           COMPACTION FLOW                                    │
└─────────────────────────────────────────────────────────────────────────────┘

  Compactor Node         Coordinator            Storage
       │                      │                    │
       │ 1. Try acquire       │                    │
       │    compaction lock   │                    │
       │─────────────────────►│                    │
       │                      │                    │
       │◄─────────────────────│                    │
       │    Lock acquired     │                    │
       │                      │                    │
       │ 2. List L0 files     │                    │
       │──────────────────────────────────────────►│
       │                      │                    │
       │◄─────────────────────────────────────────│
       │    [L0_001, L0_002, L0_003, L0_004]      │
       │                      │                    │
       │ 3. Download files    │                    │
       │──────────────────────────────────────────►│
       │                      │                    │
       │ 4. Merge & compact   │                    │
       │    (local CPU)       │                    │
       │                      │                    │
       │ 5. Upload L1 file    │                    │
       │──────────────────────────────────────────►│
       │                      │                    │
       │ 6. Delete L0 files   │                    │
       │──────────────────────────────────────────►│
       │                      │                    │
       │ 7. Invalidate cache  │                    │
       │─────────────────────►│                    │
       │                      │                    │
       │ 8. Release lock      │                    │
       │─────────────────────►│                    │
       │                      │                    │
```

---

## 5. Coordination Layer

### 5.1 Coordination Requirements

| Requirement | Purpose | Implementation |
|-------------|---------|----------------|
| **Write Ordering** | Prevent concurrent writes to same partition | Distributed locks |
| **Cache Invalidation** | Notify nodes of new/changed files | Pub/sub |
| **Leader Election** | Single compaction coordinator | Leader election |
| **Health Tracking** | Monitor node liveness | Heartbeats |

### 5.2 Coordination Options

**Option A: etcd (Recommended for Production)**
```toml
[cluster.coordinator]
type = "etcd"
endpoints = ["etcd1:2379", "etcd2:2379", "etcd3:2379"]
lock_prefix = "/stats/locks/"
cache_prefix = "/stats/cache/"
```

**Option B: Redis (Simpler Setup)**
```toml
[cluster.coordinator]
type = "redis"
url = "redis://redis-cluster:6379"
lock_prefix = "stats:locks:"
cache_channel = "stats:cache:invalidate"
```

**Option C: File-Based (Development/Simple)**
```toml
[cluster.coordinator]
type = "file"
lock_dir = "/shared/locks"
```

### 5.3 Lock Protocol

```rust
pub struct DistributedLock {
    coordinator: Arc<dyn Coordinator>,
    lock_id: String,
    holder: String,
    ttl: Duration,
}

impl DistributedLock {
    /// Try to acquire lock, returns immediately
    pub async fn try_acquire(&self) -> Result<bool, CoordError> {
        self.coordinator.try_lock(&self.lock_id, &self.holder, self.ttl).await
    }

    /// Acquire lock, blocks until acquired or timeout
    pub async fn acquire(&self, timeout: Duration) -> Result<(), CoordError> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.try_acquire().await? {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(CoordError::Timeout)
    }

    /// Release lock
    pub async fn release(&self) -> Result<(), CoordError> {
        self.coordinator.unlock(&self.lock_id, &self.holder).await
    }

    /// Extend lock TTL
    pub async fn extend(&self) -> Result<(), CoordError> {
        self.coordinator.extend_lock(&self.lock_id, &self.holder, self.ttl).await
    }
}
```

### 5.4 Cache Invalidation Protocol

```rust
/// Cache invalidation message
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheInvalidation {
    /// Affected paths (glob patterns supported)
    pub paths: Vec<String>,
    /// Invalidation reason
    pub reason: InvalidationReason,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Source node
    pub source_node: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InvalidationReason {
    /// New file uploaded
    NewFile,
    /// File deleted
    FileDeleted,
    /// File replaced (compaction)
    FileReplaced,
    /// Full cache clear
    FullClear,
}

/// Cache invalidation handler
pub struct CacheInvalidationHandler {
    cache: Arc<LocalCache>,
    subscription: Subscription,
}

impl CacheInvalidationHandler {
    pub async fn run(&self) {
        while let Some(msg) = self.subscription.next().await {
            match msg.reason {
                InvalidationReason::NewFile => {
                    // No action needed, will be fetched on next query
                }
                InvalidationReason::FileDeleted |
                InvalidationReason::FileReplaced => {
                    for path in &msg.paths {
                        self.cache.invalidate(path).await;
                    }
                }
                InvalidationReason::FullClear => {
                    self.cache.clear().await;
                }
            }
        }
    }
}
```

---

## 6. Configuration

### 6.1 Full Configuration Reference

```toml
#==============================================================================
# Stats Service Cluster Configuration (Shared Storage)
#==============================================================================

[cluster]
# Enable cluster mode
enabled = true

# Unique node identifier (auto-generated if not specified)
node_id = "node-1"

# Architecture type
architecture = "shared-storage"

#------------------------------------------------------------------------------
# Shared Storage Configuration
#------------------------------------------------------------------------------
[cluster.storage]
# Storage type: "s3", "gcs", "azure", "minio", "file"
type = "s3"

# Bucket/container name
bucket = "stats-service-data"

# Prefix within bucket
prefix = "production/"

# Region (for cloud storage)
region = "us-east-1"

# Endpoint override (for MinIO or custom S3-compatible)
# endpoint = "http://minio:9000"

# Credentials (or use IAM roles/environment)
# access_key_id = "..."
# secret_access_key = "..."

# Connection settings
max_connections = 100
connect_timeout_ms = 5000
request_timeout_ms = 30000

# Retry settings
max_retries = 3
retry_backoff_ms = 100

#------------------------------------------------------------------------------
# Write Buffer Configuration
#------------------------------------------------------------------------------
[cluster.write_buffer]
# Maximum buffer size per table before flush (bytes)
max_size_bytes = 104857600  # 100 MB

# Maximum time before flush (seconds)
flush_interval_secs = 30

# Enable local WAL for durability
enable_local_wal = true

# Local WAL directory
wal_dir = "/var/lib/stats/wal"

#------------------------------------------------------------------------------
# Local Cache Configuration
#------------------------------------------------------------------------------
[cluster.cache]
# Enable local disk cache
enabled = true

# Cache directory
cache_dir = "/var/lib/stats/cache"

# Maximum cache size (bytes)
max_size_bytes = 107374182400  # 100 GB

# Cache eviction policy: "lru", "lfu", "fifo"
eviction_policy = "lru"

# Prefetch on startup
prefetch_on_startup = true

# Prefetch patterns (glob)
prefetch_patterns = ["*/date=*/L1_*.parquet", "*/date=*/L2_*.parquet"]

#------------------------------------------------------------------------------
# Coordinator Configuration
#------------------------------------------------------------------------------
[cluster.coordinator]
# Coordinator type: "etcd", "redis", "file"
type = "etcd"

# etcd endpoints
endpoints = ["etcd1:2379", "etcd2:2379", "etcd3:2379"]

# Key prefixes
lock_prefix = "/stats/locks/"
cache_prefix = "/stats/cache/"
leader_prefix = "/stats/leader/"

# Lock settings
lock_ttl_secs = 30
lock_retry_interval_ms = 100

# Heartbeat interval
heartbeat_interval_secs = 5

#------------------------------------------------------------------------------
# Compaction Configuration
#------------------------------------------------------------------------------
[cluster.compaction]
# Enable compaction on this node
enabled = true

# Only run compaction if elected leader
leader_only = true

# Compaction schedule (cron format)
schedule = "0 3 * * *"  # 3 AM daily

# Maximum concurrent compaction jobs
max_concurrent = 4

# Temporary directory for compaction work
temp_dir = "/var/lib/stats/tmp"

#------------------------------------------------------------------------------
# Query Configuration
#------------------------------------------------------------------------------
[cluster.query]
# Maximum concurrent queries
max_concurrent = 100

# Query timeout
timeout_secs = 300

# Enable query result caching
result_cache_enabled = true
result_cache_ttl_secs = 60

# Prefetch parquet metadata
prefetch_metadata = true

#------------------------------------------------------------------------------
# Health and Monitoring
#------------------------------------------------------------------------------
[cluster.health]
# Health check interval
check_interval_secs = 10

# Unhealthy threshold (missed checks)
unhealthy_threshold = 3

# Storage health check
storage_check_enabled = true

# Coordinator health check
coordinator_check_enabled = true
```

### 6.2 Minimal Configuration

```toml
[cluster]
enabled = true
architecture = "shared-storage"

[cluster.storage]
type = "s3"
bucket = "my-stats-bucket"
region = "us-east-1"

[cluster.coordinator]
type = "etcd"
endpoints = ["etcd:2379"]
```

### 6.3 Development Configuration

```toml
[cluster]
enabled = true
architecture = "shared-storage"

[cluster.storage]
type = "file"
bucket = "/tmp/stats-data"

[cluster.coordinator]
type = "file"
endpoints = ["/tmp/stats-locks"]

[cluster.cache]
cache_dir = "/tmp/stats-cache"
max_size_bytes = 1073741824  # 1 GB
```

---

## 7. Implementation Plan

### 7.1 Phase Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Phase 1: Storage Abstraction                                                │
│  ├── Object storage interface                                               │
│  ├── S3/MinIO implementation                                                │
│  ├── Storage path utilities                                                 │
│  └── File listing and metadata                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 2: Write Path                                                         │
│  ├── Write buffer implementation                                            │
│  ├── Flush to object storage                                                │
│  ├── Local WAL (optional)                                                   │
│  └── Write acknowledgment                                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 3: Query Path                                                         │
│  ├── Local cache implementation                                             │
│  ├── Cache-through query execution                                          │
│  ├── Parquet metadata caching                                               │
│  └── Query performance optimization                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 4: Coordination                                                       │
│  ├── Coordinator interface                                                  │
│  ├── etcd/Redis implementation                                              │
│  ├── Distributed locks                                                      │
│  └── Cache invalidation                                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 5: Compaction                                                         │
│  ├── Remote compaction workflow                                             │
│  ├── Leader election for compaction                                         │
│  ├── Compaction scheduling                                                  │
│  └── Cleanup and validation                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 6: Operations                                                         │
│  ├── Health checks                                                          │
│  ├── Metrics and monitoring                                                 │
│  ├── Documentation                                                          │
│  └── Testing                                                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Module Structure

```
src/
├── storage/
│   ├── mod.rs
│   ├── interface.rs          # ObjectStorage trait
│   ├── s3.rs                 # S3/MinIO implementation
│   ├── gcs.rs                # GCS implementation
│   ├── azure.rs              # Azure Blob implementation
│   ├── local.rs              # Local filesystem
│   └── path.rs               # Storage path utilities
│
├── cluster/
│   ├── mod.rs
│   ├── config.rs             # Cluster configuration
│   │
│   ├── write_buffer/
│   │   ├── mod.rs
│   │   ├── buffer.rs         # In-memory buffer
│   │   ├── flusher.rs        # Background flusher
│   │   └── wal.rs            # Local WAL
│   │
│   ├── cache/
│   │   ├── mod.rs
│   │   ├── local_cache.rs    # LRU disk cache
│   │   ├── prefetch.rs       # Prefetch logic
│   │   └── invalidation.rs   # Cache invalidation
│   │
│   ├── coordinator/
│   │   ├── mod.rs
│   │   ├── interface.rs      # Coordinator trait
│   │   ├── etcd.rs           # etcd implementation
│   │   ├── redis.rs          # Redis implementation
│   │   ├── file.rs           # File-based (dev)
│   │   └── locks.rs          # Distributed lock helpers
│   │
│   └── compaction/
│       ├── mod.rs
│       ├── remote.rs         # Remote compaction logic
│       ├── leader.rs         # Leader election
│       └── scheduler.rs      # Compaction scheduling
│
└── ... (existing modules)
```

### 7.3 Deliverables per Phase

| Phase | Deliverables | Acceptance Criteria |
|-------|--------------|---------------------|
| **Phase 1** | Storage abstraction, S3 client | Read/write parquet to S3 |
| **Phase 2** | Write buffer, flush logic | Writes persist to S3 |
| **Phase 3** | Local cache, query integration | Queries use cached files |
| **Phase 4** | Coordinator, locks, invalidation | Multi-node cache coherence |
| **Phase 5** | Remote compaction | Compaction works with S3 |
| **Phase 6** | Health, metrics, docs | Production ready |

---

## 8. Comparison with Shared-Nothing

### 8.1 Feature Comparison

| Feature | Shared-Nothing | Shared-Storage |
|---------|----------------|----------------|
| **Complexity** | High | Medium |
| **Data Replication** | Application-level (Raft) | Storage-level (S3 durability) |
| **Node Scaling** | Requires rebalancing | Add/remove freely |
| **Write Latency** | Lower (local disk) | Higher (object storage) |
| **Query Latency** | Lower (local data) | Depends on cache hit rate |
| **Cost** | Higher (replicated storage) | Lower (single copy + S3) |
| **Operational Burden** | Higher | Lower |
| **Failure Recovery** | Complex (catch-up) | Simple (restart) |
| **Consistency** | Strong (Raft) | Eventual (cache invalidation) |

### 8.2 When to Choose Each

**Choose Shared-Storage When:**
- Cloud-native deployment (AWS, GCP, Azure)
- Cost efficiency is important
- Operational simplicity is priority
- Write latency tolerance > 100ms
- Elastic scaling is required
- Team has limited distributed systems experience

**Choose Shared-Nothing When:**
- Ultra-low latency required (< 10ms writes)
- Strong consistency is mandatory
- On-premises deployment without object storage
- High write throughput with predictable latency
- Complex multi-shard transactions needed

### 8.3 Performance Expectations

| Metric | Shared-Nothing | Shared-Storage |
|--------|----------------|----------------|
| Write Latency (p50) | 5-10ms | 50-100ms |
| Write Latency (p99) | 20-50ms | 200-500ms |
| Query Latency (cache hit) | 10-50ms | 10-50ms |
| Query Latency (cache miss) | 10-50ms | 100-500ms |
| Write Throughput | 300K pts/s/node | 100K pts/s/node |
| Query Throughput | 1000 qps/node | 1000 qps/node |

---

## 9. Operational Guide

### 9.1 Deployment

**Docker Compose (Development):**
```yaml
version: '3.8'

services:
  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data

  etcd:
    image: quay.io/coreos/etcd:v3.5.0
    command:
      - etcd
      - --name=etcd0
      - --advertise-client-urls=http://etcd:2379
      - --listen-client-urls=http://0.0.0.0:2379
    ports:
      - "2379:2379"

  stats-node-1:
    image: stats-service:latest
    ports:
      - "7000:7000"
      - "7001:7001"
    environment:
      - STATS_NODE_ID=node-1
      - STATS_STORAGE_ENDPOINT=http://minio:9000
      - STATS_COORDINATOR_ENDPOINTS=etcd:2379
    volumes:
      - node1-cache:/var/lib/stats/cache

  stats-node-2:
    image: stats-service:latest
    ports:
      - "7002:7000"
      - "7003:7001"
    environment:
      - STATS_NODE_ID=node-2
      - STATS_STORAGE_ENDPOINT=http://minio:9000
      - STATS_COORDINATOR_ENDPOINTS=etcd:2379
    volumes:
      - node2-cache:/var/lib/stats/cache

volumes:
  minio-data:
  node1-cache:
  node2-cache:
```

**Kubernetes (Production):**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stats-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: stats-service
  template:
    metadata:
      labels:
        app: stats-service
    spec:
      containers:
      - name: stats-service
        image: stats-service:latest
        ports:
        - containerPort: 7000
        - containerPort: 7001
        env:
        - name: STATS_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: STATS_STORAGE_TYPE
          value: "s3"
        - name: STATS_STORAGE_BUCKET
          value: "stats-production"
        - name: AWS_REGION
          value: "us-east-1"
        volumeMounts:
        - name: cache
          mountPath: /var/lib/stats/cache
      volumes:
      - name: cache
        emptyDir:
          sizeLimit: 100Gi
---
apiVersion: v1
kind: Service
metadata:
  name: stats-service
spec:
  type: LoadBalancer
  ports:
  - name: http
    port: 7000
  - name: flight
    port: 7001
  selector:
    app: stats-service
```

### 9.2 Monitoring

**Key Metrics:**

| Metric | Description | Alert |
|--------|-------------|-------|
| `stats_cache_hit_ratio` | Cache hit rate | < 0.7 |
| `stats_cache_size_bytes` | Current cache size | > 90% max |
| `stats_write_buffer_size` | Buffer size | > 80% max |
| `stats_flush_duration_seconds` | Flush latency | > 10s |
| `stats_storage_request_duration` | S3 latency | > 1s |
| `stats_coordinator_connected` | Coordinator health | 0 |

**Health Endpoint:**
```
GET /health

{
  "status": "healthy",
  "node_id": "node-1",
  "storage": {
    "status": "connected",
    "bucket": "stats-production",
    "latency_ms": 45
  },
  "coordinator": {
    "status": "connected",
    "type": "etcd",
    "latency_ms": 12
  },
  "cache": {
    "status": "healthy",
    "size_bytes": 53687091200,
    "hit_ratio": 0.87
  },
  "write_buffer": {
    "tables": 12,
    "size_bytes": 52428800
  }
}
```

### 9.3 Troubleshooting

| Issue | Symptoms | Resolution |
|-------|----------|------------|
| **High query latency** | Cache misses, slow S3 | Increase cache size, warm cache |
| **Write failures** | S3 upload errors | Check S3 connectivity, credentials |
| **Stale data** | Old data in queries | Check cache invalidation, etcd health |
| **Compaction not running** | L0 files accumulating | Check leader election, locks |
| **Node not joining** | Cannot reach coordinator | Check etcd connectivity |

### 9.4 Backup and Recovery

**Backup:**
- Object storage is the backup (S3 versioning, cross-region replication)
- Enable S3 versioning for point-in-time recovery
- Use S3 lifecycle policies for cost management

**Recovery:**
- Nodes are stateless - just restart
- Cache rebuilds automatically
- WAL replay for in-flight writes (if enabled)

```bash
# Recover from node failure
kubectl delete pod stats-service-xxx  # Just delete and let K8s recreate

# Recover from storage corruption (with versioning)
aws s3api list-object-versions --bucket stats-production --prefix "data/"
aws s3api get-object --bucket stats-production --key "data/..." --version-id "xxx" recovered.parquet
```

---

## Appendix A: API Reference

### Cluster-Specific Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cluster/status` | GET | Cluster status |
| `/cluster/cache/stats` | GET | Cache statistics |
| `/cluster/cache/clear` | POST | Clear local cache |
| `/cluster/storage/health` | GET | Storage health check |

### Response Examples

**GET /cluster/status**
```json
{
  "architecture": "shared-storage",
  "node_id": "node-1",
  "storage": {
    "type": "s3",
    "bucket": "stats-production",
    "connected": true
  },
  "coordinator": {
    "type": "etcd",
    "connected": true,
    "is_leader": true
  },
  "cache": {
    "size_bytes": 53687091200,
    "max_size_bytes": 107374182400,
    "entries": 1523,
    "hit_ratio": 0.87
  }
}
```

---

*This document describes the Shared-Storage Architecture alternative for Stats Service Cluster Edition.*
