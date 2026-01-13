# Proposal: Master-Slave HA with Shared Object Storage

**Document Version:** 1.0
**Date:** 2026-01-13
**Status:** Draft
**Author:** Stats Service Team

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [How It Works](#3-how-it-works)
4. [Failover Mechanism](#4-failover-mechanism)
5. [Configuration](#5-configuration)
6. [Implementation Plan](#6-implementation-plan)
7. [Comparison with Other Architectures](#7-comparison-with-other-architectures)
8. [Operational Guide](#8-operational-guide)

---

## 1. Executive Summary

This proposal describes a **Master-Slave HA architecture using Shared Object Storage**. This hybrid approach combines:

- **Master-Slave simplicity**: Only 2 nodes, clear roles, easy to understand
- **Shared Storage durability**: S3/MinIO handles data persistence and durability
- **Instant failover**: No data synchronization needed - both nodes see the same data

### Key Benefits

| Benefit | Description |
|---------|-------------|
| **No Replication Lag** | Both nodes read from same storage |
| **Instant Failover** | Slave can take over immediately |
| **Zero Data Loss** | Data persisted to S3 before ACK |
| **Simple Operations** | No WAL shipping, no catch-up sync |
| **Cost Effective** | S3 durability, no local replicas |

### Architecture at a Glance

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   MASTER-SLAVE WITH SHARED STORAGE                           │
└─────────────────────────────────────────────────────────────────────────────┘

                              Clients
                                 │
                                 ▼
                         ┌─────────────┐
                         │ Load Balancer│
                         └──────┬──────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
                ▼                               ▼
         ┌─────────────┐                 ┌─────────────┐
         │   MASTER    │                 │   SLAVE     │
         │             │                 │             │
         │ • Writes ✓  │                 │ • Writes ✗  │
         │ • Reads ✓   │                 │ • Reads ✓   │
         │ • Compact ✓ │                 │ • Standby   │
         └──────┬──────┘                 └──────┬──────┘
                │                               │
                │      ┌─────────────┐          │
                └─────►│   Shared    │◄─────────┘
                       │   Storage   │
                       │  (S3/MinIO) │
                       └─────────────┘
                              │
                       ┌──────┴──────┐
                       │    etcd     │
                       │ (Leader Lock)│
                       └─────────────┘

  Key Insight: Both nodes access the SAME data in S3!
  No replication needed - failover is instant!
```

---

## 2. Architecture Overview

### 2.1 Design Principles

1. **Single Writer**: Only master accepts writes (prevents conflicts)
2. **Shared Data**: All data stored in object storage (S3/MinIO)
3. **Local Cache**: Each node caches hot data locally for performance
4. **Leader Lock**: etcd ensures only one master at a time
5. **Instant Failover**: Slave becomes master without data sync

### 2.2 Component Responsibilities

| Component | Master | Slave |
|-----------|--------|-------|
| **Accept Writes** | ✓ | ✗ (reject or forward) |
| **Accept Reads** | ✓ | ✓ |
| **Flush to S3** | ✓ | ✗ |
| **Run Compaction** | ✓ | ✗ |
| **Hold Leader Lock** | ✓ | ✗ |
| **Monitor Master** | ✗ | ✓ |
| **Local Cache** | ✓ | ✓ |

### 2.3 Why This Works

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     WHY NO REPLICATION IS NEEDED                             │
└─────────────────────────────────────────────────────────────────────────────┘

  Traditional Master-Slave:

      Master ─────────────────► Slave
              WAL Replication
              (Async/Sync)

      Problem: Replication lag, data loss risk, complexity

  ─────────────────────────────────────────────────────────────────────────────

  Shared Storage Master-Slave:

      Master ────┐          ┌──── Slave
                 │          │
                 ▼          ▼
              ┌─────────────────┐
              │   S3 / MinIO    │
              │                 │
              │  • Data files   │
              │  • WAL files    │
              │  • Metadata     │
              └─────────────────┘

      Benefit: Both see same data instantly!
               No replication = No lag = No data loss!
```

### 2.4 Node Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              MASTER NODE                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                         HTTP / Flight API                              │ │
│  │                        (Writes + Reads)                                │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│          ┌─────────────────────────┼─────────────────────────┐              │
│          │                         │                         │              │
│          ▼                         ▼                         ▼              │
│  ┌───────────────┐        ┌───────────────┐        ┌───────────────┐       │
│  │ Write Buffer  │        │ Query Engine  │        │   Compactor   │       │
│  │ (In-Memory)   │        │               │        │               │       │
│  └───────┬───────┘        └───────┬───────┘        └───────┬───────┘       │
│          │                        │                        │                │
│          │                        ▼                        │                │
│          │                ┌───────────────┐                │                │
│          │                │  Local Cache  │                │                │
│          │                │  (LRU Disk)   │                │                │
│          │                └───────┬───────┘                │                │
│          │                        │                        │                │
│          └────────────────────────┼────────────────────────┘                │
│                                   │                                          │
│                                   ▼                                          │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                      S3 Client (Read + Write)                          │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                      Leader Lock Manager (etcd)                        │ │
│  │                      - Holds master lock                               │ │
│  │                      - Renews periodically                             │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────────┐
│                              SLAVE NODE                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                         HTTP / Flight API                              │ │
│  │                        (Reads Only)                                    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│                                    ▼                                         │
│                           ┌───────────────┐                                 │
│                           │ Query Engine  │                                 │
│                           │               │                                 │
│                           └───────┬───────┘                                 │
│                                   │                                          │
│                                   ▼                                          │
│                           ┌───────────────┐                                 │
│                           │  Local Cache  │                                 │
│                           │  (LRU Disk)   │                                 │
│                           └───────┬───────┘                                 │
│                                   │                                          │
│                                   ▼                                          │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                      S3 Client (Read Only)                             │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                      Failover Monitor                                  │ │
│  │                      - Watches master lock                             │ │
│  │                      - Acquires lock if master fails                   │ │
│  │                      - Promotes self to master                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. How It Works

### 3.1 Write Path

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              WRITE PATH                                      │
└─────────────────────────────────────────────────────────────────────────────┘

  Client                    Master                         S3
    │                         │                             │
    │  POST /write/db         │                             │
    │────────────────────────►│                             │
    │                         │                             │
    │                         │ 1. Verify master lock       │
    │                         │    (local cache, fast)      │
    │                         │                             │
    │                         │ 2. Buffer in memory         │
    │                         │                             │
    │                         │ 3. Write to S3 WAL          │
    │                         │    (for crash recovery)     │
    │                         │────────────────────────────►│
    │                         │                             │
    │                         │◄────────────────────────────│
    │                         │         ACK                 │
    │                         │                             │
    │◄────────────────────────│                             │
    │       204 OK            │                             │
    │                         │                             │
    │                         │     ... flush interval ...  │
    │                         │                             │
    │                         │ 4. Convert to Parquet       │
    │                         │                             │
    │                         │ 5. Upload to S3             │
    │                         │────────────────────────────►│
    │                         │                             │
    │                         │ 6. Delete WAL segment       │
    │                         │────────────────────────────►│
    │                         │                             │
    │                         │ 7. Invalidate caches        │
    │                         │    (notify via etcd)        │
    │                         │                             │
```

### 3.2 Read Path

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              READ PATH                                       │
└─────────────────────────────────────────────────────────────────────────────┘

  Client               Master or Slave                   S3
    │                         │                           │
    │  GET /query/db          │                           │
    │  SELECT * FROM...       │                           │
    │────────────────────────►│                           │
    │                         │                           │
    │                         │ 1. Check local cache      │
    │                         │                           │
    │                         │    Cache HIT:             │
    │                         │    └─ Return cached data  │
    │                         │                           │
    │                         │    Cache MISS:            │
    │                         │    └─ Fetch from S3       │
    │                         │───────────────────────────►│
    │                         │                           │
    │                         │◄───────────────────────────│
    │                         │       File content        │
    │                         │                           │
    │                         │ 2. Cache locally          │
    │                         │                           │
    │                         │ 3. Execute query          │
    │                         │    (DataFusion)           │
    │                         │                           │
    │◄────────────────────────│                           │
    │       Results           │                           │
    │                         │                           │


  Note: Both Master and Slave can serve reads!
        They access the SAME data in S3.
```

### 3.3 Cache Invalidation

When master writes new data, slave caches may become stale:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CACHE INVALIDATION                                   │
└─────────────────────────────────────────────────────────────────────────────┘

     Master                    etcd                     Slave
       │                         │                        │
       │ 1. Flush new parquet    │                        │
       │    file to S3           │                        │
       │                         │                        │
       │ 2. Publish invalidation │                        │
       │────────────────────────►│                        │
       │    key: /cache/invalid  │                        │
       │    val: {path, ts}      │                        │
       │                         │                        │
       │                         │ 3. Watch notification  │
       │                         │───────────────────────►│
       │                         │                        │
       │                         │                        │ 4. Invalidate
       │                         │                        │    local cache
       │                         │                        │
       │                         │                        │ 5. Next query
       │                         │                        │    fetches fresh
       │                         │                        │    data from S3
```

**Alternative: TTL-based Cache**

For simpler setups, use time-based cache expiry:

```toml
[cache]
# Cache entries expire after 60 seconds
# Slave may serve slightly stale data (acceptable for many use cases)
ttl_secs = 60
```

---

## 4. Failover Mechanism

### 4.1 Leader Lock

The master holds a lock in etcd that must be renewed periodically:

```rust
pub struct LeaderLock {
    /// etcd client
    client: EtcdClient,
    /// Lock key
    key: String,
    /// Lock TTL
    ttl: Duration,
    /// Lease ID
    lease_id: i64,
}

impl LeaderLock {
    /// Try to acquire master lock
    pub async fn try_acquire(&mut self) -> Result<bool, Error> {
        // Create lease with TTL
        let lease = self.client.lease_grant(self.ttl.as_secs() as i64).await?;
        self.lease_id = lease.id();

        // Try to put key with lease (only succeeds if key doesn't exist)
        let txn = self.client.txn()
            .when(&[Compare::create_revision(&self.key, CompareOp::Equal, 0)])
            .and_then(&[Op::put(&self.key, "master", Some(PutOptions::new().with_lease(self.lease_id)))])
            .or_else(&[]);

        let resp = self.client.txn(txn).await?;
        Ok(resp.succeeded())
    }

    /// Renew lock (must be called before TTL expires)
    pub async fn renew(&self) -> Result<(), Error> {
        self.client.lease_keep_alive(self.lease_id).await?;
        Ok(())
    }

    /// Release lock
    pub async fn release(&self) -> Result<(), Error> {
        self.client.lease_revoke(self.lease_id).await?;
        Ok(())
    }
}
```

### 4.2 Failover Process

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FAILOVER PROCESS                                   │
└─────────────────────────────────────────────────────────────────────────────┘

  Timeline when Master crashes:

  T=0s     Master crashes
           └─ Stop renewing lock

  T=10s    Lock TTL expires (default: 10s)
           └─ etcd automatically deletes lock key

  T=10.1s  Slave detects lock is gone
           └─ Watching etcd for lock key deletion

  T=10.2s  Slave acquires lock
           └─ Becomes new master

  T=10.3s  Slave starts accepting writes

  T=10.5s  HAProxy health check detects new master
           └─ Routes traffic to slave

  ─────────────────────────────────────────────────────────────────────────────

  Total failover time: ~10-15 seconds
  Data loss: ZERO (all data in S3)

  ─────────────────────────────────────────────────────────────────────────────


                  Master                              Slave
                    │                                   │
    Running ────────┼───────────────────────────────────┼──────── Standby
                    │                                   │
                    │ Heartbeat (renew lock)            │
                    │─────────────────►                 │
                    │                                   │
                    │ Heartbeat (renew lock)            │
                    │─────────────────►                 │
                    │                                   │
                    X CRASH                             │
                                                        │
                    │ (lock TTL counting down)          │
                    │                                   │
                    │ Lock expires!                     │ Watch triggered
                    │                                   │
                                                        │ Try acquire lock
                                                        │─────────────────►
                                                        │
                                                        │ Lock acquired!
                                                        │◄─────────────────
                                                        │
                                              ──────────┼──────── Master
                                                        │
                                                        │ Accept writes
                                                        │
```

### 4.3 Slave Promotion Logic

```rust
pub struct FailoverManager {
    lock: LeaderLock,
    role: AtomicRole,
    s3_client: Arc<S3Client>,
}

impl FailoverManager {
    /// Main failover loop (runs on slave)
    pub async fn run(&self) {
        loop {
            match self.role.load() {
                Role::Slave => {
                    // Watch for master lock to disappear
                    if self.lock.watch_deleted().await {
                        // Master lock gone - try to become master
                        if self.try_promote().await.is_ok() {
                            info!("Promoted to master!");
                        }
                    }
                }
                Role::Master => {
                    // Keep renewing lock
                    if let Err(e) = self.lock.renew().await {
                        error!("Failed to renew lock: {}", e);
                        self.demote().await;
                    }
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
    }

    async fn try_promote(&self) -> Result<(), Error> {
        // 1. Try to acquire master lock
        if !self.lock.try_acquire().await? {
            return Err(Error::LockNotAcquired);
        }

        // 2. Recover any uncommitted WAL from S3
        self.recover_wal().await?;

        // 3. Switch role to master
        self.role.store(Role::Master);

        // 4. Start accepting writes
        Ok(())
    }

    async fn recover_wal(&self) -> Result<(), Error> {
        // List WAL files in S3
        let wal_files = self.s3_client.list("wal/").await?;

        for wal_file in wal_files {
            // Replay WAL entries that weren't flushed
            let entries = self.parse_wal(&wal_file).await?;
            for entry in entries {
                self.apply_entry(entry).await?;
            }
            // Delete processed WAL
            self.s3_client.delete(&wal_file.path).await?;
        }

        Ok(())
    }
}
```

### 4.4 No Split-Brain Guarantee

The etcd lock ensures only one master at a time:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      SPLIT-BRAIN PREVENTION                                  │
└─────────────────────────────────────────────────────────────────────────────┘

  Scenario: Network partition between Master and Slave

      Master                                    Slave
         │                                        │
         │    ════════════════════════════════    │
         │         Network Partition              │
         │    ════════════════════════════════    │
         │                                        │
         │ Cannot reach etcd                      │ Can reach etcd
         │ Cannot renew lock                      │ Sees lock expire
         │                                        │
         │ Lock expires!                          │ Acquires lock
         │ MUST stop writes                       │ Becomes master
         │                                        │

  Result: Old master stops, new master starts
          Only ONE master ever writes to S3

  ─────────────────────────────────────────────────────────────────────────────

  Master's self-check before every write:

  fn accept_write(&self) -> bool {
      // Check if we still hold the lock
      if !self.lock.is_held() {
          return false;  // Reject write
      }

      // Check lock hasn't expired locally
      if self.lock.time_since_renew() > LOCK_TTL / 2 {
          return false;  // Reject write (being conservative)
      }

      true
  }
```

---

## 5. Configuration

### 5.1 Master Configuration

```toml
#==============================================================================
# Stats Service - Master Node (Shared Storage Mode)
#==============================================================================

[server]
http_port = 7000
flight_port = 7001

#------------------------------------------------------------------------------
# Shared Storage Configuration
#------------------------------------------------------------------------------
[storage]
# Storage mode: "local" or "shared"
mode = "shared"

# Object storage settings
type = "s3"                          # "s3", "minio", "gcs"
bucket = "stats-service-data"
region = "us-east-1"
# endpoint = "http://minio:9000"     # For MinIO

# Prefix for this cluster
prefix = "production/"

#------------------------------------------------------------------------------
# High Availability Configuration
#------------------------------------------------------------------------------
[ha]
enabled = true
role = "master"
node_id = "master-01"

#------------------------------------------------------------------------------
# Leader Lock (etcd)
#------------------------------------------------------------------------------
[ha.lock]
# etcd endpoints
etcd_endpoints = ["etcd:2379"]

# Lock key
key = "/stats-service/master-lock"

# Lock TTL (seconds)
ttl_secs = 10

# Renew interval (should be < ttl/2)
renew_interval_secs = 3

#------------------------------------------------------------------------------
# Write Buffer
#------------------------------------------------------------------------------
[ha.write_buffer]
# Maximum buffer size before flush
max_size_bytes = 104857600  # 100MB

# Maximum time before flush
flush_interval_secs = 30

# Write to S3 WAL before ACK (for durability)
wal_enabled = true

#------------------------------------------------------------------------------
# Local Cache
#------------------------------------------------------------------------------
[cache]
enabled = true
dir = "/var/lib/stats/cache"
max_size_bytes = 53687091200  # 50GB

# Cache invalidation method: "etcd" or "ttl"
invalidation = "etcd"
```

### 5.2 Slave Configuration

```toml
#==============================================================================
# Stats Service - Slave Node (Shared Storage Mode)
#==============================================================================

[server]
http_port = 7000
flight_port = 7001

#------------------------------------------------------------------------------
# Shared Storage Configuration (same as master)
#------------------------------------------------------------------------------
[storage]
mode = "shared"
type = "s3"
bucket = "stats-service-data"
region = "us-east-1"
prefix = "production/"

#------------------------------------------------------------------------------
# High Availability Configuration
#------------------------------------------------------------------------------
[ha]
enabled = true
role = "slave"
node_id = "slave-01"

# What to do with write requests
# "reject" - return 503
# "forward" - forward to master
write_handling = "reject"

#------------------------------------------------------------------------------
# Failover Settings
#------------------------------------------------------------------------------
[ha.failover]
# Automatic promotion when master fails
enabled = true

# How often to check master lock
check_interval_secs = 1

#------------------------------------------------------------------------------
# Leader Lock (same etcd cluster)
#------------------------------------------------------------------------------
[ha.lock]
etcd_endpoints = ["etcd:2379"]
key = "/stats-service/master-lock"
ttl_secs = 10

#------------------------------------------------------------------------------
# Local Cache
#------------------------------------------------------------------------------
[cache]
enabled = true
dir = "/var/lib/stats/cache"
max_size_bytes = 53687091200  # 50GB
invalidation = "etcd"
```

### 5.3 Minimal Configuration

**Master:**
```toml
[storage]
mode = "shared"
bucket = "my-bucket"

[ha]
enabled = true
role = "master"

[ha.lock]
etcd_endpoints = ["etcd:2379"]
```

**Slave:**
```toml
[storage]
mode = "shared"
bucket = "my-bucket"

[ha]
enabled = true
role = "slave"

[ha.lock]
etcd_endpoints = ["etcd:2379"]
```

---

## 6. Implementation Plan

### 6.1 Phase Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Phase 1: Shared Storage Integration                                         │
│  ├── S3 client wrapper                                                      │
│  ├── Write buffer with S3 flush                                             │
│  ├── S3-based WAL                                                           │
│  └── Query engine with S3 backend                                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 2: Leader Lock                                                        │
│  ├── etcd lock implementation                                               │
│  ├── Lock renewal loop                                                      │
│  ├── Write rejection without lock                                           │
│  └── Graceful lock release                                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 3: Failover                                                           │
│  ├── Lock watching (slave)                                                  │
│  ├── Automatic promotion                                                    │
│  ├── WAL recovery on promotion                                              │
│  └── Role switching                                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 4: Cache & Operations                                                 │
│  ├── Local cache with invalidation                                          │
│  ├── Health endpoints                                                       │
│  ├── Metrics                                                                │
│  └── Documentation                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Module Structure

```
src/
├── storage/
│   ├── mod.rs
│   ├── backend.rs          # Storage backend trait
│   ├── local.rs            # Local filesystem (existing)
│   ├── s3.rs               # S3/MinIO backend
│   └── wal.rs              # S3-based WAL
│
├── ha/
│   ├── mod.rs
│   ├── config.rs           # HA configuration
│   ├── role.rs             # Role management
│   ├── lock.rs             # etcd leader lock
│   ├── failover.rs         # Failover logic
│   └── cache_invalidation.rs
│
└── ... (existing modules)
```

### 6.3 Key Changes to Existing Code

| Module | Changes |
|--------|---------|
| `cache.rs` | Flush to S3 instead of local disk |
| `compactor.rs` | Read/write parquet from S3 |
| `query_engine.rs` | Register S3 tables in DataFusion |
| `main.rs` | Add HA initialization, lock management |

---

## 7. Comparison with Other Architectures

### 7.1 Architecture Comparison

| Feature | Master-Slave (Local) | Master-Slave (Shared) | Full Cluster |
|---------|---------------------|----------------------|--------------|
| **Nodes** | 2 | 2 | 3+ |
| **Replication** | WAL streaming | None (shared S3) | Raft/WAL |
| **Data Loss Risk** | Async: possible | Zero | Near-zero |
| **Failover Time** | ~10-30s | ~10s | ~10-15s |
| **Replication Lag** | Yes | No | Yes |
| **Complexity** | Medium | **Low** | High |
| **Cost** | 2x storage | **1x storage + S3** | 3x+ storage |
| **Write Latency** | Low | Medium (S3) | Low |
| **Read Scaling** | Limited | **Both nodes** | Yes |

### 7.2 When to Choose This Architecture

**Choose Master-Slave with Shared Storage when:**

- ✅ You need simple, reliable HA
- ✅ Zero data loss is required
- ✅ You're running in cloud (S3/GCS available)
- ✅ Write latency of 50-100ms is acceptable
- ✅ You want to minimize operational complexity
- ✅ Read scaling to 2 nodes is sufficient

**Don't choose this architecture when:**

- ❌ Ultra-low write latency required (<10ms)
- ❌ No object storage available
- ❌ Need more than 2 nodes for reads
- ❌ Need horizontal write scaling

### 7.3 Performance Expectations

| Metric | Master-Slave (Local) | Master-Slave (Shared) |
|--------|---------------------|----------------------|
| Write Latency (p50) | 5-10ms | 50-100ms |
| Write Latency (p99) | 20-50ms | 150-300ms |
| Read Latency (cache hit) | 5-20ms | 5-20ms |
| Read Latency (cache miss) | 5-20ms | 50-200ms |
| Failover Time | 15-30s | 10-15s |
| Data Loss (failover) | Possible (async) | **Zero** |

---

## 8. Operational Guide

### 8.1 Docker Compose Setup

```yaml
version: '3.8'

services:
  # Coordination service
  etcd:
    image: quay.io/coreos/etcd:v3.5.0
    container_name: etcd
    command:
      - etcd
      - --advertise-client-urls=http://etcd:2379
      - --listen-client-urls=http://0.0.0.0:2379
    ports:
      - "2379:2379"
    networks:
      - stats-net

  # Object storage
  minio:
    image: minio/minio
    container_name: minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data
    networks:
      - stats-net

  # Master node
  master:
    image: stats-service:latest
    container_name: master
    ports:
      - "7000:7000"
      - "7001:7001"
    environment:
      - STATS_STORAGE_MODE=shared
      - STATS_STORAGE_TYPE=minio
      - STATS_STORAGE_ENDPOINT=http://minio:9000
      - STATS_STORAGE_BUCKET=stats-data
      - STATS_STORAGE_ACCESS_KEY=minioadmin
      - STATS_STORAGE_SECRET_KEY=minioadmin
      - STATS_HA_ENABLED=true
      - STATS_HA_ROLE=master
      - STATS_HA_ETCD_ENDPOINTS=etcd:2379
    volumes:
      - master-cache:/var/lib/stats/cache
    networks:
      - stats-net
    depends_on:
      - etcd
      - minio

  # Slave node
  slave:
    image: stats-service:latest
    container_name: slave
    ports:
      - "7010:7000"
      - "7011:7001"
    environment:
      - STATS_STORAGE_MODE=shared
      - STATS_STORAGE_TYPE=minio
      - STATS_STORAGE_ENDPOINT=http://minio:9000
      - STATS_STORAGE_BUCKET=stats-data
      - STATS_STORAGE_ACCESS_KEY=minioadmin
      - STATS_STORAGE_SECRET_KEY=minioadmin
      - STATS_HA_ENABLED=true
      - STATS_HA_ROLE=slave
      - STATS_HA_ETCD_ENDPOINTS=etcd:2379
    volumes:
      - slave-cache:/var/lib/stats/cache
    networks:
      - stats-net
    depends_on:
      - etcd
      - minio
      - master

  # Load balancer
  haproxy:
    image: haproxy:2.8
    container_name: haproxy
    ports:
      - "80:80"
      - "8404:8404"
    volumes:
      - ./haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro
    networks:
      - stats-net
    depends_on:
      - master
      - slave

networks:
  stats-net:
    driver: bridge

volumes:
  minio-data:
  master-cache:
  slave-cache:
```

### 8.2 Verify Setup

```bash
# Start cluster
docker-compose up -d

# Create bucket in MinIO
docker-compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker-compose exec minio mc mb local/stats-data

# Check master status
curl http://localhost:7000/ha/status
# {
#   "role": "master",
#   "lock_held": true,
#   "storage": {"type": "s3", "bucket": "stats-data"}
# }

# Check slave status
curl http://localhost:7010/ha/status
# {
#   "role": "slave",
#   "lock_held": false,
#   "watching_master": true
# }

# Write data
curl -X POST "http://localhost/write/mydb" \
  -d "cpu,host=server01 usage=42.5"

# Query from master
curl "http://localhost:7000/query/mydb" -d "SELECT * FROM cpu"

# Query from slave (same data!)
curl "http://localhost:7010/query/mydb" -d "SELECT * FROM cpu"
```

### 8.3 Failover Test

```bash
# Simulate master failure
docker-compose stop master

# Watch slave status
watch -n 1 'curl -s http://localhost:7010/ha/status | jq .'

# After ~10 seconds:
# {
#   "role": "master",        <- Was "slave"
#   "lock_held": true,       <- Acquired lock
#   "promoted_at": "..."
# }

# Writes now work on slave (new master)
curl -X POST "http://localhost:7010/write/mydb" \
  -d "cpu,host=server01 usage=55.0"

# HAProxy automatically routes to new master
curl "http://localhost/query/mydb" -d "SELECT * FROM cpu"
```

### 8.4 Recovery

```bash
# Start old master - becomes slave automatically
docker-compose start master

# Check status
curl http://localhost:7000/ha/status
# {
#   "role": "slave",         <- Detected existing master
#   "lock_held": false
# }
```

### 8.5 Monitoring

**Key Metrics:**

| Metric | Description | Alert |
|--------|-------------|-------|
| `stats_ha_role` | Current role (0=slave, 1=master) | Change |
| `stats_ha_lock_held` | Whether lock is held | Master without lock |
| `stats_ha_lock_ttl_remaining_secs` | Time until lock expires | < 3s |
| `stats_s3_write_latency_ms` | S3 write latency | > 500ms |
| `stats_cache_hit_ratio` | Local cache hit rate | < 0.5 |

**Health Endpoint:**

```json
GET /ha/status

{
  "node_id": "master-01",
  "role": "master",
  "state": "running",
  "lock": {
    "held": true,
    "key": "/stats-service/master-lock",
    "ttl_remaining_secs": 7,
    "last_renewed": "2026-01-13T12:00:00Z"
  },
  "storage": {
    "type": "s3",
    "bucket": "stats-data",
    "connected": true,
    "latency_ms": 45
  },
  "cache": {
    "size_bytes": 10737418240,
    "hit_ratio": 0.85
  }
}
```

---

## Appendix A: Comparison Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ALL HA ARCHITECTURE OPTIONS                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. Master-Slave (Local Storage)                                            │
│     ├── Complexity: Medium                                                  │
│     ├── Data Loss: Possible (async) or None (sync)                         │
│     ├── Failover: 15-30 seconds                                            │
│     └── Best for: On-premises, low latency requirements                    │
│                                                                              │
│  2. Master-Slave (Shared Storage) ← THIS PROPOSAL                          │
│     ├── Complexity: LOW                                                     │
│     ├── Data Loss: ZERO                                                     │
│     ├── Failover: 10-15 seconds                                            │
│     └── Best for: Cloud, simple HA, zero data loss                         │
│                                                                              │
│  3. Shared-Nothing Cluster                                                  │
│     ├── Complexity: High                                                    │
│     ├── Data Loss: Near-zero                                               │
│     ├── Failover: 10-15 seconds                                            │
│     └── Best for: Scale + HA, on-premises                                  │
│                                                                              │
│  4. Shared-Storage Cluster                                                  │
│     ├── Complexity: Medium                                                  │
│     ├── Data Loss: Zero                                                     │
│     ├── Failover: Instant (stateless)                                      │
│     └── Best for: Cloud, elastic scaling                                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

*This document describes the Master-Slave HA architecture using Shared Object Storage for Stats Service.*
