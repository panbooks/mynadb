# Proposal: Stats Service Master-Slave High Availability

**Document Version:** 1.0
**Date:** 2026-01-13
**Status:** Draft
**Author:** Stats Service Team

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Replication Design](#3-replication-design)
4. [Failover Mechanism](#4-failover-mechanism)
5. [Configuration](#5-configuration)
6. [Implementation Plan](#6-implementation-plan)
7. [Operational Guide](#7-operational-guide)

---

## 1. Executive Summary

This proposal describes a simple **Master-Slave High Availability** architecture for Stats Service. Unlike full clustering solutions, this approach focuses solely on availability through replication, assuming:

- Single node has sufficient write throughput capacity
- Single node has sufficient disk capacity
- Primary goal is **zero downtime** when master fails

### Key Characteristics

| Characteristic | Value |
|----------------|-------|
| **Complexity** | Low |
| **Nodes Required** | 2 (master + slave) |
| **Write Path** | Master only |
| **Read Path** | Master or Slave |
| **Failover Time** | < 30 seconds |
| **Data Loss Risk** | Configurable (sync/async) |

### Architecture at a Glance

```
                         ┌─────────────────┐
                         │  Load Balancer  │
                         │  (VIP / DNS)    │
                         └────────┬────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
             ┌─────────────┐             ┌─────────────┐
             │   MASTER    │────────────►│   SLAVE     │
             │             │ Replication │             │
             │  • Writes   │             │  • Standby  │
             │  • Reads    │             │  • Reads    │
             │  • Compact  │             │  (optional) │
             └─────────────┘             └─────────────┘
                    │                           │
                    ▼                           ▼
             ┌─────────────┐             ┌─────────────┐
             │  Local Disk │             │  Local Disk │
             │  (Primary)  │             │  (Replica)  │
             └─────────────┘             └─────────────┘
```

---

## 2. Architecture Overview

### 2.1 Design Goals

1. **Simplicity**: Minimal changes to existing single-node architecture
2. **High Availability**: Automatic failover on master failure
3. **Data Durability**: No data loss (sync mode) or minimal loss (async mode)
4. **Read Scaling**: Optional read queries on slave

### 2.2 Node Roles

| Role | Responsibilities |
|------|------------------|
| **Master** | Accept writes, execute queries, run compaction, replicate to slave |
| **Slave** | Receive replication, standby for failover, optional read queries |

### 2.3 Component Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              MASTER NODE                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                         HTTP / Flight API                              │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│          ┌─────────────────────────┼─────────────────────────┐              │
│          │                         │                         │              │
│          ▼                         ▼                         ▼              │
│  ┌───────────────┐        ┌───────────────┐        ┌───────────────┐       │
│  │ Write Handler │        │ Query Engine  │        │   Compactor   │       │
│  └───────┬───────┘        └───────────────┘        └───────────────┘       │
│          │                                                                   │
│          ▼                                                                   │
│  ┌───────────────┐                                                          │
│  │  Replication  │─────────────────────────────────────────────────────┐    │
│  │    Sender     │                                                     │    │
│  └───────────────┘                                                     │    │
│                                                                        │    │
└────────────────────────────────────────────────────────────────────────│────┘
                                                                         │
                                    Replication Stream (WAL + Files)     │
                                                                         │
┌────────────────────────────────────────────────────────────────────────│────┐
│                              SLAVE NODE                                │    │
├────────────────────────────────────────────────────────────────────────│────┤
│                                                                        │    │
│  ┌───────────────┐                                                     │    │
│  │  Replication  │◄────────────────────────────────────────────────────┘    │
│  │   Receiver    │                                                          │
│  └───────┬───────┘                                                          │
│          │                                                                   │
│          ▼                                                                   │
│  ┌───────────────┐        ┌───────────────┐        ┌───────────────┐       │
│  │  WAL Applier  │        │ Query Engine  │        │   Failover    │       │
│  │               │        │ (Read-only)   │        │   Monitor     │       │
│  └───────────────┘        └───────────────┘        └───────────────┘       │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                     HTTP / Flight API (Optional)                       │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Replication Design

### 3.1 What Gets Replicated

| Data Type | Method | Consistency |
|-----------|--------|-------------|
| **WAL Entries** | Stream replication | Real-time |
| **Parquet Files** | File sync after flush | After flush |
| **Metadata** | Included in WAL | Real-time |

### 3.2 Replication Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           REPLICATION FLOW                                   │
└─────────────────────────────────────────────────────────────────────────────┘

  Client              Master                              Slave
    │                   │                                   │
    │ POST /write/db    │                                   │
    │──────────────────►│                                   │
    │                   │                                   │
    │                   │ 1. Write to local WAL             │
    │                   │                                   │
    │                   │ 2. Send WAL entry                 │
    │                   │──────────────────────────────────►│
    │                   │                                   │
    │                   │                    3. Write to    │
    │                   │                       local WAL   │
    │                   │                                   │
    │                   │◄──────────────────────────────────│
    │                   │         ACK (sync mode)           │
    │                   │                                   │
    │                   │ 4. Write to cache                 │
    │                   │                                   │
    │◄──────────────────│                                   │
    │     204 OK        │                                   │
    │                   │                                   │
    │                   │     ... flush interval ...        │
    │                   │                                   │
    │                   │ 5. Flush to Parquet               │
    │                   │                                   │
    │                   │ 6. Send file notification         │
    │                   │──────────────────────────────────►│
    │                   │                                   │
    │                   │                    7. Fetch file  │
    │                   │◄──────────────────────────────────│
    │                   │                                   │
    │                   │ 8. Send file content              │
    │                   │──────────────────────────────────►│
    │                   │                                   │
```

### 3.3 Replication Modes

**Synchronous Replication (Default - No Data Loss)**

```
Write Request
     │
     ▼
┌─────────────┐
│ Master WAL  │
└──────┬──────┘
       │
       ▼
┌─────────────┐     ┌─────────────┐
│   Send to   │────►│  Slave WAL  │
│   Slave     │     │             │
└──────┬──────┘     └──────┬──────┘
       │                   │
       │◄──────────────────┘
       │        ACK
       ▼
┌─────────────┐
│  ACK Client │
└─────────────┘

RPO = 0 (no data loss)
Write latency = Master + Network RTT + Slave
```

**Asynchronous Replication (Higher Performance)**

```
Write Request
     │
     ▼
┌─────────────┐
│ Master WAL  │
└──────┬──────┘
       │
       ├──────────────────┐
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│  ACK Client │    │   Send to   │──► Slave WAL
└─────────────┘    │   Slave     │    (async)
                   └─────────────┘

RPO = flush_interval (potential data loss)
Write latency = Master only
```

**Semi-Synchronous Replication (Balanced)**

```
Write Request
     │
     ▼
┌─────────────┐
│ Master WAL  │
└──────┬──────┘
       │
       ▼
┌─────────────┐     ┌─────────────┐
│   Send to   │────►│  Slave      │
│   Slave     │     │  Receive    │
└──────┬──────┘     └─────────────┘
       │
       │◄─── ACK (received, not persisted)
       │
       ▼
┌─────────────┐
│  ACK Client │
└─────────────┘

RPO ≈ 0 (slave has data in memory)
Write latency = Master + Network RTT
```

### 3.4 WAL Replication Protocol

```rust
/// Replication message types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReplicationMessage {
    /// WAL entry to replicate
    WalEntry {
        sequence: u64,
        timestamp: DateTime<Utc>,
        catalog: String,
        data: Vec<u8>,
    },

    /// New parquet file available
    FileCreated {
        path: String,
        size: u64,
        checksum: String,
    },

    /// File deleted (compaction/retention)
    FileDeleted {
        path: String,
    },

    /// Heartbeat
    Heartbeat {
        sequence: u64,
        timestamp: DateTime<Utc>,
    },

    /// Request file content
    FileRequest {
        path: String,
    },

    /// File content response
    FileContent {
        path: String,
        data: Vec<u8>,
        checksum: String,
    },
}

/// Replication acknowledgment
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationAck {
    pub sequence: u64,
    pub success: bool,
    pub error: Option<String>,
}
```

### 3.5 Replication State Machine

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      SLAVE REPLICATION STATE                                 │
└─────────────────────────────────────────────────────────────────────────────┘

                         ┌───────────────┐
                         │  DISCONNECTED │
                         └───────┬───────┘
                                 │ Connect to master
                                 ▼
                         ┌───────────────┐
                         │  CONNECTING   │
                         └───────┬───────┘
                                 │ Connection established
                                 ▼
                         ┌───────────────┐
                         │   SYNCING     │◄──────────────┐
                         │ (Catch-up)    │               │
                         └───────┬───────┘               │
                                 │ Caught up             │ Lag detected
                                 ▼                       │
                         ┌───────────────┐               │
                         │  STREAMING    │───────────────┘
                         │ (Real-time)   │
                         └───────┬───────┘
                                 │ Master unreachable
                                 ▼
                         ┌───────────────┐
                         │   WAITING     │
                         │ (For failover)│
                         └───────┬───────┘
                                 │ Promoted to master
                                 ▼
                         ┌───────────────┐
                         │    MASTER     │
                         │  (Promoted)   │
                         └───────────────┘
```

---

## 4. Failover Mechanism

### 4.1 Failure Detection

```rust
pub struct FailureDetector {
    /// Heartbeat interval from master
    heartbeat_interval: Duration,

    /// Number of missed heartbeats before failure
    failure_threshold: u32,

    /// Last heartbeat received
    last_heartbeat: Instant,

    /// Current missed count
    missed_count: AtomicU32,
}

impl FailureDetector {
    pub fn is_master_failed(&self) -> bool {
        let elapsed = self.last_heartbeat.elapsed();
        let threshold = self.heartbeat_interval * self.failure_threshold;
        elapsed > threshold
    }

    pub fn time_until_failover(&self) -> Duration {
        let elapsed = self.last_heartbeat.elapsed();
        let threshold = self.heartbeat_interval * self.failure_threshold;
        if elapsed >= threshold {
            Duration::ZERO
        } else {
            threshold - elapsed
        }
    }
}
```

**Detection Timeline:**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FAILURE DETECTION TIMELINE                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  heartbeat_interval = 5s                                                     │
│  failure_threshold = 3                                                       │
│                                                                              │
│  T=0s     Master sends heartbeat                                            │
│  T=5s     Master sends heartbeat                                            │
│  T=10s    Master CRASHES                                                    │
│  T=15s    Heartbeat missed (1)                                              │
│  T=20s    Heartbeat missed (2)                                              │
│  T=25s    Heartbeat missed (3) → FAILOVER TRIGGERED                         │
│                                                                              │
│  Total detection time: 15 seconds                                           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Failover Process

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          FAILOVER PROCESS                                    │
└─────────────────────────────────────────────────────────────────────────────┘

  Slave                    Coordinator              Load Balancer
    │                          │                         │
    │ 1. Detect master         │                         │
    │    failure               │                         │
    │                          │                         │
    │ 2. Verify failure        │                         │
    │    (try reconnect)       │                         │
    │                          │                         │
    │ 3. Apply remaining       │                         │
    │    WAL entries           │                         │
    │                          │                         │
    │ 4. Acquire master        │                         │
    │    lock                  │                         │
    │─────────────────────────►│                         │
    │                          │                         │
    │◄─────────────────────────│                         │
    │    Lock granted          │                         │
    │                          │                         │
    │ 5. Promote self          │                         │
    │    to master             │                         │
    │                          │                         │
    │ 6. Update endpoint       │                         │
    │─────────────────────────────────────────────────────►
    │                          │                         │
    │                          │     7. Health check     │
    │◄─────────────────────────────────────────────────────
    │                          │                         │
    │─────────────────────────────────────────────────────►
    │    OK - Ready            │                         │
    │                          │                         │
    │ 8. Accept writes         │     8. Route traffic   │
    │    and queries           │        to new master   │
    │                          │                         │
```

### 4.3 Failover Modes

**Automatic Failover (Recommended)**

```toml
[ha.failover]
mode = "automatic"
detection_interval_secs = 5
failure_threshold = 3
promotion_delay_secs = 5
```

- Slave automatically promotes when master fails
- Requires coordination service to prevent split-brain
- Best for most production deployments

**Manual Failover**

```toml
[ha.failover]
mode = "manual"
```

- Operator manually promotes slave via API
- Safest option, no risk of split-brain
- Requires 24/7 operations team

```bash
# Manual failover command
curl -X POST http://slave:7000/ha/promote
```

**Semi-Automatic Failover**

```toml
[ha.failover]
mode = "semi-automatic"
require_confirmation = true
confirmation_timeout_secs = 300
```

- Slave detects failure and prepares for promotion
- Waits for operator confirmation (or timeout)
- Balance between safety and automation

### 4.4 Split-Brain Prevention

To prevent both nodes acting as master:

**Option 1: Coordination Service (etcd/Consul)**

```
┌─────────────┐                    ┌─────────────┐
│   Node A    │                    │   Node B    │
│             │                    │             │
│ Try acquire │                    │ Try acquire │
│ master lock │                    │ master lock │
└──────┬──────┘                    └──────┬──────┘
       │                                  │
       └──────────────┬───────────────────┘
                      │
                      ▼
               ┌─────────────┐
               │    etcd     │
               │             │
               │ Only ONE    │
               │ can hold    │
               │ the lock    │
               └─────────────┘
```

**Option 2: STONITH (Shoot The Other Node In The Head)**

```toml
[ha.stonith]
enabled = true
method = "power_off"  # "power_off", "network_isolate", "api_call"
target_host = "192.168.1.100"
```

Before promoting, slave ensures old master is truly dead:
- Power off via IPMI/BMC
- Network isolation via SDN
- Cloud API (stop instance)

**Option 3: Quorum with Witness**

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Master    │     │   Slave     │     │   Witness   │
│             │     │             │     │  (Arbiter)  │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┴───────────────────┘
                           │
                    Vote for leader
                    (2 of 3 required)
```

Witness is a lightweight node that only participates in voting.

---

## 5. Configuration

### 5.1 Master Configuration

```toml
#==============================================================================
# Stats Service Master Configuration
#==============================================================================

[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "/data/stats"

#------------------------------------------------------------------------------
# High Availability - Master Settings
#------------------------------------------------------------------------------
[ha]
# Enable HA mode
enabled = true

# Node role: "master" or "slave"
role = "master"

# Unique node identifier
node_id = "master-01"

#------------------------------------------------------------------------------
# Replication Settings (Master)
#------------------------------------------------------------------------------
[ha.replication]
# Replication mode: "sync", "async", "semi-sync"
mode = "sync"

# Listen address for replication connections
listen_addr = "0.0.0.0:7002"

# Maximum replication lag before blocking writes (sync mode)
max_lag_bytes = 104857600  # 100 MB

# Replication timeout
timeout_secs = 30

# Compression for replication stream
compression = "lz4"  # "none", "lz4", "zstd"

#------------------------------------------------------------------------------
# Heartbeat Settings
#------------------------------------------------------------------------------
[ha.heartbeat]
# Heartbeat interval
interval_secs = 5

# Include replication status in heartbeat
include_status = true

#------------------------------------------------------------------------------
# Slave Connection (Master tracks connected slaves)
#------------------------------------------------------------------------------
[ha.slaves]
# Minimum slaves required for sync replication
min_slaves = 1

# Wait for slave on startup
wait_for_slave = false
wait_timeout_secs = 60
```

### 5.2 Slave Configuration

```toml
#==============================================================================
# Stats Service Slave Configuration
#==============================================================================

[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "/data/stats"

#------------------------------------------------------------------------------
# High Availability - Slave Settings
#------------------------------------------------------------------------------
[ha]
enabled = true
role = "slave"
node_id = "slave-01"

#------------------------------------------------------------------------------
# Replication Settings (Slave)
#------------------------------------------------------------------------------
[ha.replication]
# Master address
master_addr = "master-host:7002"

# Reconnect settings
reconnect_interval_secs = 5
max_reconnect_attempts = 0  # 0 = unlimited

# Local WAL directory for received entries
wal_dir = "/data/stats/wal"

#------------------------------------------------------------------------------
# Failover Settings
#------------------------------------------------------------------------------
[ha.failover]
# Failover mode: "automatic", "manual", "semi-automatic"
mode = "automatic"

# Failure detection
heartbeat_timeout_secs = 5
failure_threshold = 3

# Promotion delay (wait before promoting to allow master recovery)
promotion_delay_secs = 10

# Verify master is truly dead before promoting
verify_master_down = true
verify_timeout_secs = 5

#------------------------------------------------------------------------------
# Split-Brain Prevention
#------------------------------------------------------------------------------
[ha.coordination]
# Coordination type: "etcd", "consul", "file", "none"
type = "etcd"

# etcd settings
etcd_endpoints = ["etcd:2379"]
lock_key = "/stats/master-lock"
lock_ttl_secs = 30

#------------------------------------------------------------------------------
# Read Queries on Slave (Optional)
#------------------------------------------------------------------------------
[ha.read_replica]
# Allow read queries on slave
enabled = true

# Advertise as read replica
advertise = true

# Maximum acceptable replication lag for reads
max_lag_secs = 60
```

### 5.3 Minimal Configuration

**Master (minimal):**
```toml
[ha]
enabled = true
role = "master"

[ha.replication]
mode = "sync"
listen_addr = "0.0.0.0:7002"
```

**Slave (minimal):**
```toml
[ha]
enabled = true
role = "slave"

[ha.replication]
master_addr = "master:7002"

[ha.failover]
mode = "automatic"
```

---

## 6. Implementation Plan

### 6.1 Phase Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Phase 1: Replication Infrastructure                                         │
│  ├── Replication protocol messages                                          │
│  ├── TCP connection management                                              │
│  ├── WAL streaming from master                                              │
│  └── WAL receiving on slave                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 2: Data Synchronization                                               │
│  ├── Initial sync (full copy)                                               │
│  ├── Incremental sync (WAL replay)                                          │
│  ├── File replication (parquet files)                                       │
│  └── Consistency verification                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 3: Failover Mechanism                                                 │
│  ├── Failure detection                                                      │
│  ├── Automatic promotion                                                    │
│  ├── Split-brain prevention                                                 │
│  └── Traffic redirection                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 4: Operations & Monitoring                                            │
│  ├── HA status API                                                          │
│  ├── Replication lag metrics                                                │
│  ├── Manual failover/failback                                               │
│  └── Documentation                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Module Structure

```
src/
├── ha/
│   ├── mod.rs
│   ├── config.rs              # HA configuration
│   ├── role.rs                # Role management (master/slave)
│   │
│   ├── replication/
│   │   ├── mod.rs
│   │   ├── protocol.rs        # Replication messages
│   │   ├── sender.rs          # Master: send replication stream
│   │   ├── receiver.rs        # Slave: receive and apply
│   │   ├── connection.rs      # TCP connection management
│   │   └── sync.rs            # Initial/incremental sync
│   │
│   ├── failover/
│   │   ├── mod.rs
│   │   ├── detector.rs        # Failure detection
│   │   ├── promoter.rs        # Slave promotion logic
│   │   └── coordinator.rs     # Split-brain prevention
│   │
│   └── monitor/
│       ├── mod.rs
│       ├── status.rs          # HA status tracking
│       └── metrics.rs         # Replication metrics
│
└── ... (existing modules)
```

### 6.3 Key Interfaces

```rust
/// HA Manager - main interface
pub struct HaManager {
    config: HaConfig,
    role: AtomicRole,
    replication: Option<ReplicationManager>,
    failover: Option<FailoverManager>,
}

impl HaManager {
    /// Get current role
    pub fn role(&self) -> Role;

    /// Check if this node can accept writes
    pub fn can_accept_writes(&self) -> bool;

    /// Get replication status
    pub fn replication_status(&self) -> ReplicationStatus;

    /// Manually promote slave to master
    pub async fn promote(&self) -> Result<(), HaError>;

    /// Manually demote master to slave
    pub async fn demote(&self, new_master: &str) -> Result<(), HaError>;
}

/// Replication status
#[derive(Clone, Debug)]
pub struct ReplicationStatus {
    pub role: Role,
    pub state: ReplicationState,
    pub master_addr: Option<String>,
    pub slave_addrs: Vec<String>,
    pub lag_bytes: u64,
    pub lag_seconds: f64,
    pub last_sequence: u64,
    pub last_sync: DateTime<Utc>,
}
```

### 6.4 Deliverables

| Phase | Deliverables | Acceptance Criteria |
|-------|--------------|---------------------|
| **Phase 1** | Replication protocol, streaming | WAL entries flow master→slave |
| **Phase 2** | Full sync, file replication | Slave has complete data copy |
| **Phase 3** | Failover, promotion | Slave becomes master on failure |
| **Phase 4** | API, metrics, docs | Production ready |

---

## 7. Operational Guide

### 7.1 Deployment

**Docker Compose:**

```yaml
version: '3.8'

services:
  etcd:
    image: quay.io/coreos/etcd:v3.5.0
    command:
      - etcd
      - --advertise-client-urls=http://etcd:2379
      - --listen-client-urls=http://0.0.0.0:2379
    ports:
      - "2379:2379"

  master:
    image: stats-service:latest
    hostname: master
    ports:
      - "7000:7000"
      - "7001:7001"
      - "7002:7002"
    volumes:
      - master-data:/data/stats
      - ./master-config.toml:/etc/stats/config.toml
    environment:
      - STATS_HA_ROLE=master

  slave:
    image: stats-service:latest
    hostname: slave
    ports:
      - "7010:7000"
      - "7011:7001"
    volumes:
      - slave-data:/data/stats
      - ./slave-config.toml:/etc/stats/config.toml
    environment:
      - STATS_HA_ROLE=slave
      - STATS_HA_MASTER_ADDR=master:7002
    depends_on:
      - master
      - etcd

  # Load balancer for automatic failover
  haproxy:
    image: haproxy:latest
    ports:
      - "80:7000"
    volumes:
      - ./haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg
    depends_on:
      - master
      - slave

volumes:
  master-data:
  slave-data:
```

**HAProxy Configuration:**

```
# haproxy.cfg
global
    daemon

defaults
    mode http
    timeout connect 5s
    timeout client 30s
    timeout server 30s

frontend stats_frontend
    bind *:7000
    default_backend stats_backend

backend stats_backend
    option httpchk GET /health
    http-check expect status 200

    # Primary master
    server master master:7000 check inter 2s fall 3 rise 2

    # Backup slave (only used when master is down)
    server slave slave:7000 check inter 2s fall 3 rise 2 backup
```

### 7.2 Initial Setup

```bash
# 1. Start master
docker-compose up -d master

# 2. Verify master is running
curl http://localhost:7000/health
# {"status": "healthy", "role": "master"}

# 3. Start slave
docker-compose up -d slave

# 4. Verify slave connected
curl http://localhost:7000/ha/status
# {
#   "role": "master",
#   "slaves": [{"addr": "slave:7002", "state": "streaming", "lag_bytes": 0}]
# }

curl http://localhost:7010/ha/status
# {
#   "role": "slave",
#   "master": "master:7002",
#   "state": "streaming",
#   "lag_bytes": 0
# }

# 5. Start load balancer
docker-compose up -d haproxy
```

### 7.3 Failover Testing

```bash
# 1. Check current status
curl http://localhost/ha/status

# 2. Simulate master failure
docker-compose stop master

# 3. Wait for failover (15-30 seconds)
sleep 30

# 4. Verify slave promoted
curl http://localhost:7010/ha/status
# {"role": "master", "promoted_at": "2026-01-13T..."}

# 5. Verify writes work
curl -X POST http://localhost/write/test \
  -d "cpu,host=server01 usage=42.5"
# 204 No Content

# 6. Bring back old master as new slave
docker-compose start master
# Old master will detect new master and become slave
```

### 7.4 Manual Failover

```bash
# Planned failover (e.g., for maintenance)

# 1. Check replication is caught up
curl http://localhost:7010/ha/status
# Ensure lag_bytes = 0

# 2. Promote slave
curl -X POST http://localhost:7010/ha/promote

# 3. Demote old master (optional, will auto-detect)
curl -X POST http://localhost:7000/ha/demote?new_master=slave:7002

# 4. Update load balancer or DNS
```

### 7.5 Monitoring

**Key Metrics:**

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `stats_ha_role` | Current role (0=slave, 1=master) | Change |
| `stats_ha_replication_lag_bytes` | Replication lag in bytes | > 100MB |
| `stats_ha_replication_lag_seconds` | Replication lag in seconds | > 60s |
| `stats_ha_slave_connected` | Slave connection status | 0 |
| `stats_ha_last_heartbeat_seconds` | Time since last heartbeat | > 15s |
| `stats_ha_failover_count` | Number of failovers | Increase |

**Health Endpoint:**

```
GET /ha/status

{
  "role": "master",
  "state": "running",
  "node_id": "master-01",
  "uptime_secs": 86400,
  "replication": {
    "mode": "sync",
    "slaves": [
      {
        "node_id": "slave-01",
        "addr": "10.0.0.2:7002",
        "state": "streaming",
        "lag_bytes": 1024,
        "lag_seconds": 0.5,
        "last_sequence": 1234567,
        "connected_at": "2026-01-13T00:00:00Z"
      }
    ]
  }
}
```

### 7.6 Troubleshooting

| Issue | Symptoms | Resolution |
|-------|----------|------------|
| **Slave not connecting** | `state: disconnected` | Check network, firewall, master addr |
| **High replication lag** | `lag_bytes` growing | Check network bandwidth, disk I/O |
| **Failover not triggering** | Slave stays slave | Check etcd connectivity, lock status |
| **Split-brain** | Both nodes are master | Stop one node, check coordination |
| **Slow failover** | > 30 seconds | Reduce heartbeat interval, threshold |

### 7.7 Backup and Recovery

**Backup Strategy:**

```bash
# Backup from slave (doesn't impact master)
curl -X POST http://slave:7000/backup?dest=/backup/snapshot.tar.gz
```

**Recovery Scenarios:**

| Scenario | Recovery Steps |
|----------|----------------|
| **Master disk failure** | Slave auto-promotes; replace master disk; start as slave |
| **Slave disk failure** | Replace disk; start slave; will sync from master |
| **Both nodes fail** | Restore from backup; start as master |
| **Network partition** | Wait for resolution; nodes will reconcile |

---

## Appendix A: API Reference

### HA Management Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/ha/status` | GET | Get HA status |
| `/ha/promote` | POST | Promote slave to master |
| `/ha/demote` | POST | Demote master to slave |
| `/ha/pause` | POST | Pause replication |
| `/ha/resume` | POST | Resume replication |

### Status Response

```json
{
  "role": "master|slave",
  "state": "running|syncing|waiting|promoted",
  "node_id": "string",
  "replication": {
    "mode": "sync|async|semi-sync",
    "master_addr": "string (slave only)",
    "slaves": [
      {
        "node_id": "string",
        "addr": "string",
        "state": "connecting|syncing|streaming|disconnected",
        "lag_bytes": 0,
        "lag_seconds": 0.0
      }
    ]
  },
  "failover": {
    "mode": "automatic|manual|semi-automatic",
    "last_failover": "timestamp",
    "failover_count": 0
  }
}
```

---

## Appendix B: Comparison with Other Architectures

| Feature | Master-Slave | Shared-Nothing Cluster | Shared-Storage |
|---------|--------------|------------------------|----------------|
| **Complexity** | **Low** | High | Medium |
| **Nodes Required** | **2** | 3+ | 2+ |
| **Write Scaling** | No | Yes | Yes |
| **Read Scaling** | Limited | Yes | Yes |
| **Failover Time** | ~30s | ~15s | N/A (stateless) |
| **Data Loss Risk** | Configurable | Near-zero | Near-zero |
| **Operational Burden** | **Low** | High | Medium |
| **Cost** | **Low** | High | Medium |
| **Best For** | **HA only** | Scale + HA | Cloud + HA |

---

*This document describes the Master-Slave High Availability architecture for Stats Service.*
