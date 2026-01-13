# Proposal: Stats Service Cluster Edition

**Document Version:** 1.0
**Date:** 2026-01-13
**Status:** Draft
**Author:** Stats Service Team

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Motivation and Goals](#2-motivation-and-goals)
3. [Architecture Overview](#3-architecture-overview)
4. [Detailed Design](#4-detailed-design)
   - 4.1 [Cluster Topology](#41-cluster-topology)
   - 4.2 [Data Distribution](#42-data-distribution)
   - 4.3 [Write Path](#43-write-path)
   - 4.4 [Query Path](#44-query-path)
   - 4.5 [Replication](#45-replication)
   - 4.6 [High Availability](#46-high-availability)
5. [Component Specifications](#5-component-specifications)
6. [Configuration](#6-configuration)
7. [API Changes](#7-api-changes)
8. [Implementation Plan](#8-implementation-plan)
9. [Testing Strategy](#9-testing-strategy)
10. [Migration Path](#10-migration-path)
11. [Operational Considerations](#11-operational-considerations)
12. [Risk Assessment](#12-risk-assessment)
13. [Appendix](#13-appendix)

---

## 1. Executive Summary

This proposal outlines the architecture and implementation plan for Stats Service Cluster Edition, transforming the current single-node time series database into a distributed system capable of horizontal scaling, fault tolerance, and high availability.

### Key Capabilities

| Capability | Single Node | Cluster Edition |
|------------|-------------|-----------------|
| Write Throughput | ~100K points/sec | ~1M+ points/sec (scales with nodes) |
| Storage Capacity | Single disk | Petabyte-scale |
| Availability | Single point of failure | 99.9%+ with replication |
| Query Performance | Single node | Parallel execution across shards |
| Fault Tolerance | None | Automatic failover |

### Architectural Approach

We propose a **shared-nothing architecture** with:
- **Catalog-based sharding** for natural data isolation
- **Raft-based replication** for strong consistency guarantees
- **Stateless routers** for load balancing and query distribution
- **Arrow Flight** for efficient inter-node data transfer

---

## 2. Motivation and Goals

### 2.1 Current Limitations

The single-node Stats Service faces several scalability challenges:

1. **Vertical Scaling Limits**: Single node cannot exceed hardware limits
2. **Single Point of Failure**: Node failure causes complete service outage
3. **Storage Constraints**: Limited to single machine's disk capacity
4. **Write Bottleneck**: All writes funnel through one node
5. **Query Performance**: Large analytical queries limited to single node resources

### 2.2 Goals

#### Primary Goals

| Goal | Target | Metric |
|------|--------|--------|
| **High Performance** | 10x write throughput | Points ingested per second |
| **High Availability** | 99.9% uptime | Annual downtime < 8.76 hours |
| **Horizontal Scaling** | Linear scalability | Throughput increases with nodes |
| **Data Durability** | No data loss | RPO = 0 (sync replication) |
| **Fault Tolerance** | Survive N-1 failures | With N replicas |

#### Secondary Goals

- **Operational Simplicity**: Easy deployment and management
- **Backward Compatibility**: Existing clients work without changes
- **Cost Efficiency**: Commodity hardware support
- **Cloud Native**: Kubernetes-ready deployment

### 2.3 Non-Goals

- **Global Distribution**: Multi-region deployment (future consideration)
- **Multi-Tenancy**: Tenant isolation at cluster level (future consideration)
- **ACID Transactions**: Cross-shard transactions not supported initially

---

## 3. Architecture Overview

### 3.1 High-Level Architecture

```
                                    ┌─────────────────────────────────────┐
                                    │           Load Balancer             │
                                    │         (HAProxy/Nginx/K8s)         │
                                    └─────────────────┬───────────────────┘
                                                      │
                    ┌─────────────────────────────────┼─────────────────────────────────┐
                    │                                 │                                 │
             ┌──────▼──────┐                   ┌──────▼──────┐                   ┌──────▼──────┐
             │   Router 1  │                   │   Router 2  │                   │   Router 3  │
             │ (Stateless) │                   │ (Stateless) │                   │ (Stateless) │
             └──────┬──────┘                   └──────┬──────┘                   └──────┬──────┘
                    │                                 │                                 │
                    └─────────────────────────────────┼─────────────────────────────────┘
                                                      │
                    ┌─────────────────────────────────┼─────────────────────────────────┐
                    │                                 │                                 │
             ┌──────▼──────┐                   ┌──────▼──────┐                   ┌──────▼──────┐
             │   Data Node │                   │   Data Node │                   │   Data Node │
             │   (Shard A) │◄─────────────────►│   (Shard B) │◄─────────────────►│   (Shard C) │
             │   Primary   │   Replication     │   Primary   │   Replication     │   Primary   │
             └──────┬──────┘                   └──────┬──────┘                   └──────┬──────┘
                    │                                 │                                 │
             ┌──────▼──────┐                   ┌──────▼──────┐                   ┌──────▼──────┐
             │   Replica   │                   │   Replica   │                   │   Replica   │
             │   (Shard A) │                   │   (Shard B) │                   │   (Shard C) │
             └─────────────┘                   └─────────────┘                   └─────────────┘
                                                      │
                                    ┌─────────────────┴───────────────────┐
                                    │        Metadata Service             │
                                    │     (etcd / Embedded Raft)          │
                                    └─────────────────────────────────────┘
```

### 3.2 Node Roles

| Role | Responsibilities | Stateful | Scalability |
|------|-----------------|----------|-------------|
| **Router** | Request routing, load balancing, query planning | No | Horizontal |
| **Data Node** | Data storage, local query execution, replication | Yes | Horizontal |
| **Metadata** | Cluster coordination, shard mapping, leader election | Yes | 3-5 nodes |

### 3.3 Component Interactions

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              ROUTER NODE                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   HTTP      │  │   Flight    │  │   Query     │  │   Routing Table     │ │
│  │   Server    │  │   Server    │  │   Planner   │  │   (Shard Map)       │ │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘ │
│         │                │                │                     │            │
│         └────────────────┴────────────────┴─────────────────────┘            │
│                                   │                                          │
└───────────────────────────────────┼──────────────────────────────────────────┘
                                    │ Arrow Flight / gRPC
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA NODE                                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │    WAL      │  │   Cache     │  │   Query     │  │   Replication       │ │
│  │   Writer    │  │   Manager   │  │   Engine    │  │   Manager           │ │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘ │
│         │                │                │                     │            │
│  ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐  ┌──────────▼──────────┐ │
│  │  Parquet    │  │  Compactor  │  │  DataFusion │  │   Raft Protocol     │ │
│  │  Storage    │  │             │  │             │  │                     │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Detailed Design

### 4.1 Cluster Topology

#### 4.1.1 Node Discovery

Nodes discover each other through one of three mechanisms:

**Static Configuration:**
```toml
[cluster.discovery]
method = "static"
seeds = ["10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000"]
```

**DNS-based Discovery:**
```toml
[cluster.discovery]
method = "dns"
dns_name = "stats-service.cluster.local"
dns_poll_interval_secs = 30
```

**etcd-based Discovery:**
```toml
[cluster.discovery]
method = "etcd"
etcd_endpoints = ["10.0.0.10:2379"]
etcd_prefix = "/stats-service/nodes/"
```

#### 4.1.2 Node Lifecycle

```
                    ┌─────────────┐
                    │   JOINING   │
                    │  (Initial)  │
                    └──────┬──────┘
                           │ Metadata sync complete
                           ▼
                    ┌─────────────┐
        ┌──────────│   SYNCING   │──────────┐
        │          │ (Catch-up)  │          │
        │          └──────┬──────┘          │
        │                 │ WAL caught up   │ Sync failed
        │                 ▼                 ▼
        │          ┌─────────────┐   ┌─────────────┐
        │          │   ACTIVE    │   │   FAILED    │
        │          │  (Serving)  │   │             │
        │          └──────┬──────┘   └──────┬──────┘
        │                 │                 │
        │                 │ Graceful        │ Recovery
        │                 │ shutdown        │ initiated
        │                 ▼                 │
        │          ┌─────────────┐          │
        └─────────►│   LEAVING   │◄─────────┘
                   │(Draining)   │
                   └──────┬──────┘
                          │ Drain complete
                          ▼
                   ┌─────────────┐
                   │    LEFT     │
                   │  (Removed)  │
                   └─────────────┘
```

#### 4.1.3 Cluster Membership Protocol

```rust
/// Node state in the cluster
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub advertise_addr: SocketAddr,
    pub role: NodeRole,
    pub state: NodeState,
    pub shards: Vec<ShardAssignment>,
    pub capacity: NodeCapacity,
    pub last_heartbeat: DateTime<Utc>,
    pub version: String,
}

/// Heartbeat message exchanged between nodes
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    pub node_id: String,
    pub term: u64,
    pub timestamp: DateTime<Utc>,
    pub load: NodeLoad,
    pub shard_status: Vec<ShardStatus>,
}
```

### 4.2 Data Distribution

#### 4.2.1 Sharding Strategy

We support three sharding strategies:

**Strategy 1: Catalog-Based (Recommended for Most Use Cases)**

```
┌─────────────────────────────────────────────────────────────────┐
│                        Shard Mapping                            │
├─────────────────┬─────────────────┬─────────────────────────────┤
│     Catalog     │      Shard      │          Nodes              │
├─────────────────┼─────────────────┼─────────────────────────────┤
│   metrics_db    │    shard_001    │ [node1(P), node2(R), node3(R)] │
│   logs_db       │    shard_002    │ [node2(P), node3(R), node1(R)] │
│   traces_db     │    shard_003    │ [node3(P), node1(R), node2(R)] │
└─────────────────┴─────────────────┴─────────────────────────────┘
                                    P = Primary, R = Replica
```

Pros:
- Natural isolation between databases
- Simple routing logic
- No cross-shard queries for single-database queries

Cons:
- Potential hot spots if one catalog has much more traffic
- Limited parallelism for single-catalog queries

**Strategy 2: Time-Based**

```
┌─────────────────────────────────────────────────────────────────┐
│                     Time-Based Sharding                         │
├─────────────────┬─────────────────┬─────────────────────────────┤
│   Time Range    │      Shard      │          Nodes              │
├─────────────────┼─────────────────┼─────────────────────────────┤
│ 2026-01-01~07   │    shard_w01    │ [node1(P), node2(R)]        │
│ 2026-01-08~14   │    shard_w02    │ [node2(P), node3(R)]        │
│ 2026-01-15~21   │    shard_w03    │ [node3(P), node1(R)]        │
└─────────────────┴─────────────────┴─────────────────────────────┘
```

Pros:
- Hot/cold data separation
- Easy retention management (drop old shards)
- Recent queries hit fewer shards

Cons:
- Recent time range becomes hot spot
- Requires time-based query routing

**Strategy 3: Hash-Based**

```
┌─────────────────────────────────────────────────────────────────┐
│                      Hash-Based Sharding                        │
│                                                                 │
│   hash(catalog + schema + table) % num_shards = shard_id       │
│                                                                 │
│   Example: hash("metrics_db.service_1.cpu") % 16 = 7           │
│            → Data goes to shard_007                            │
└─────────────────────────────────────────────────────────────────┘
```

Pros:
- Even distribution
- Good parallelism for cross-table queries

Cons:
- Cross-shard queries common
- Resharding is expensive

#### 4.2.2 Shard Assignment Algorithm

```python
def assign_shards(shards: List[Shard], nodes: List[Node], replication_factor: int):
    """
    Assign shards to nodes with rack/zone awareness
    """
    assignments = {}

    for shard in shards:
        # Sort nodes by load (ascending)
        available_nodes = sorted(nodes, key=lambda n: n.load)

        # Select primary (lowest load)
        primary = available_nodes[0]
        assignments[shard.id] = {
            'primary': primary.id,
            'replicas': []
        }

        # Select replicas from different racks/zones
        selected_zones = {primary.zone}
        for node in available_nodes[1:]:
            if len(assignments[shard.id]['replicas']) >= replication_factor - 1:
                break
            if node.zone not in selected_zones:
                assignments[shard.id]['replicas'].append(node.id)
                selected_zones.add(node.zone)

        # Fill remaining replicas if not enough zones
        for node in available_nodes[1:]:
            if len(assignments[shard.id]['replicas']) >= replication_factor - 1:
                break
            if node.id not in assignments[shard.id]['replicas']:
                assignments[shard.id]['replicas'].append(node.id)

    return assignments
```

#### 4.2.3 Shard Rebalancing

Rebalancing is triggered when:
1. Node joins or leaves the cluster
2. Load imbalance exceeds threshold (default: 20%)
3. Manual rebalance request

```
┌─────────────────────────────────────────────────────────────────┐
│                    Rebalancing Process                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Calculate target distribution                               │
│     ┌──────────────────────────────────────────────────────┐   │
│     │ target_shards_per_node = total_shards / num_nodes    │   │
│     └──────────────────────────────────────────────────────┘   │
│                                                                 │
│  2. Identify moves                                              │
│     ┌──────────────────────────────────────────────────────┐   │
│     │ For each overloaded node:                            │   │
│     │   Select shards to move (prefer replicas over primary)│   │
│     │   Find target nodes (underloaded, different zone)    │   │
│     └──────────────────────────────────────────────────────┘   │
│                                                                 │
│  3. Execute moves (one at a time to minimize impact)           │
│     ┌──────────────────────────────────────────────────────┐   │
│     │ a. Add new replica on target node                    │   │
│     │ b. Wait for sync to complete                         │   │
│     │ c. Remove old replica (or demote if was primary)     │   │
│     │ d. Update routing table                              │   │
│     └──────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.3 Write Path

#### 4.3.1 Write Flow

```
┌────────────────────────────────────────────────────────────────────────────┐
│                            WRITE FLOW                                       │
└────────────────────────────────────────────────────────────────────────────┘

  Client                Router               Primary              Replicas
    │                     │                    │                     │
    │  POST /write/db     │                    │                     │
    │────────────────────►│                    │                     │
    │                     │                    │                     │
    │                     │ 1. Parse request   │                     │
    │                     │ 2. Lookup shard    │                     │
    │                     │    for "db"        │                     │
    │                     │                    │                     │
    │                     │  Forward to        │                     │
    │                     │  primary           │                     │
    │                     │───────────────────►│                     │
    │                     │                    │                     │
    │                     │                    │ 3. Append to WAL    │
    │                     │                    │ 4. Write to cache   │
    │                     │                    │                     │
    │                     │                    │ 5. Replicate        │
    │                     │                    │────────────────────►│
    │                     │                    │                     │
    │                     │                    │◄────────────────────│
    │                     │                    │    ACK (async)      │
    │                     │                    │    or wait (sync)   │
    │                     │                    │                     │
    │                     │◄───────────────────│                     │
    │                     │      ACK           │                     │
    │                     │                    │                     │
    │◄────────────────────│                    │                     │
    │      204 OK         │                    │                     │
    │                     │                    │                     │
```

#### 4.3.2 Write-Ahead Log (WAL) Design

```rust
/// WAL Entry structure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalEntry {
    /// Monotonically increasing sequence number
    pub sequence: u64,
    /// Term for Raft consensus
    pub term: u64,
    /// Timestamp of the write
    pub timestamp: DateTime<Utc>,
    /// Target catalog.schema.table
    pub target: TablePath,
    /// Serialized line protocol data
    pub data: Vec<u8>,
    /// CRC32 checksum
    pub checksum: u32,
}

/// WAL Segment (one file)
pub struct WalSegment {
    pub segment_id: u64,
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub path: PathBuf,
    pub size_bytes: u64,
    pub created_at: DateTime<Utc>,
}
```

**WAL File Format:**

```
┌─────────────────────────────────────────────────────────────────┐
│                        WAL Segment File                         │
├─────────────────────────────────────────────────────────────────┤
│  Header (64 bytes)                                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Magic: "STATSWAL" (8 bytes)                             │   │
│  │ Version: u32 (4 bytes)                                  │   │
│  │ Segment ID: u64 (8 bytes)                               │   │
│  │ Start Sequence: u64 (8 bytes)                           │   │
│  │ Node ID: [u8; 32] (32 bytes)                            │   │
│  │ Reserved: 4 bytes                                       │   │
│  └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│  Entry 1                                                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Length: u32 (4 bytes)                                   │   │
│  │ Sequence: u64 (8 bytes)                                 │   │
│  │ Term: u64 (8 bytes)                                     │   │
│  │ Timestamp: i64 (8 bytes)                                │   │
│  │ Target Length: u16 (2 bytes)                            │   │
│  │ Target: [u8; target_length]                             │   │
│  │ Data Length: u32 (4 bytes)                              │   │
│  │ Data: [u8; data_length]                                 │   │
│  │ CRC32: u32 (4 bytes)                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│  Entry 2 ...                                                    │
├─────────────────────────────────────────────────────────────────┤
│  Entry N ...                                                    │
├─────────────────────────────────────────────────────────────────┤
│  Footer (32 bytes)                                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ End Sequence: u64 (8 bytes)                             │   │
│  │ Entry Count: u64 (8 bytes)                              │   │
│  │ File CRC32: u32 (4 bytes)                               │   │
│  │ Closed: u8 (1 byte, 0=open, 1=closed)                   │   │
│  │ Reserved: 11 bytes                                      │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

#### 4.3.3 Write Consistency Levels

| Level | Description | Latency | Durability |
|-------|-------------|---------|------------|
| `one` | ACK after local WAL write | Lowest | May lose on node failure |
| `quorum` | ACK after majority ACK | Medium | Survives minority failure |
| `all` | ACK after all replicas ACK | Highest | Maximum durability |

```toml
[cluster.write]
consistency = "quorum"  # "one", "quorum", "all"
timeout_ms = 5000
retry_count = 3
```

### 4.4 Query Path

#### 4.4.1 Query Flow

```
┌────────────────────────────────────────────────────────────────────────────┐
│                            QUERY FLOW                                       │
└────────────────────────────────────────────────────────────────────────────┘

  Client                Router               Data Nodes
    │                     │                    │
    │  SELECT * FROM      │                    │
    │  metrics.svc.cpu    │                    │
    │────────────────────►│                    │
    │                     │                    │
    │                     │ 1. Parse SQL       │
    │                     │ 2. Identify        │
    │                     │    catalogs        │
    │                     │ 3. Plan query      │
    │                     │                    │
    │                     │  ┌─────────────────┴──────────────────┐
    │                     │  │          SINGLE SHARD              │
    │                     │  │  (catalog maps to one shard)       │
    │                     │  └─────────────────┬──────────────────┘
    │                     │                    │
    │                     │  Forward to any    │
    │                     │  replica (load     │
    │                     │  balanced)         │
    │                     │───────────────────►│ Node 1
    │                     │                    │
    │                     │                    │ Execute locally
    │                     │                    │ with DataFusion
    │                     │                    │
    │                     │◄───────────────────│
    │                     │  Stream results    │
    │                     │  (Arrow Flight)    │
    │                     │                    │
    │◄────────────────────│                    │
    │  Results            │                    │


    │                     │  ┌─────────────────┴──────────────────┐
    │                     │  │         MULTI SHARD                │
    │                     │  │  (query spans multiple catalogs)   │
    │                     │  └─────────────────┬──────────────────┘
    │                     │                    │
    │                     │   Scatter query    │
    │                     │   to all relevant  │
    │                     │   shards           │
    │                     │───────────────────►│ Node 1
    │                     │───────────────────►│ Node 2
    │                     │───────────────────►│ Node 3
    │                     │                    │
    │                     │◄───────────────────│ Partial results
    │                     │◄───────────────────│ Partial results
    │                     │◄───────────────────│ Partial results
    │                     │                    │
    │                     │ Merge results      │
    │                     │ (streaming)        │
    │                     │                    │
    │◄────────────────────│                    │
    │  Merged results     │                    │
```

#### 4.4.2 Query Planning

```rust
/// Distributed query plan
pub enum DistributedPlan {
    /// Query can be executed on a single shard
    SingleShard {
        shard_id: ShardId,
        nodes: Vec<NodeId>,  // Any replica can execute
        plan: LogicalPlan,
    },

    /// Query must be scattered to multiple shards
    ScatterGather {
        /// Plans to execute on each shard
        shard_plans: Vec<ShardPlan>,
        /// How to merge results
        merge_plan: MergePlan,
    },
}

pub struct ShardPlan {
    pub shard_id: ShardId,
    pub nodes: Vec<NodeId>,
    pub plan: LogicalPlan,
    /// Projections to push down
    pub projections: Vec<String>,
    /// Predicates to push down
    pub predicates: Vec<Expr>,
}

pub enum MergePlan {
    /// Simple union of results
    Union,
    /// Merge sorted streams (for ORDER BY)
    MergeSort { sort_keys: Vec<SortKey> },
    /// Final aggregation (for GROUP BY)
    FinalAggregate {
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    },
}
```

#### 4.4.3 Query Optimizations

**Predicate Push-Down:**
```sql
-- Original query
SELECT * FROM db1.schema.table WHERE time > '2026-01-01' AND value > 100

-- Router pushes predicates to data nodes
-- Each node executes:
SELECT * FROM db1.schema.table WHERE time > '2026-01-01' AND value > 100
-- Only matching rows are streamed back
```

**Partial Aggregation:**
```sql
-- Original query
SELECT host, AVG(cpu) FROM metrics.default.cpu GROUP BY host

-- Data node executes (partial aggregation):
SELECT host, SUM(cpu) as sum_cpu, COUNT(*) as count_cpu
FROM metrics.default.cpu GROUP BY host

-- Router merges:
SELECT host, SUM(sum_cpu) / SUM(count_cpu) as avg_cpu
FROM partial_results GROUP BY host
```

**LIMIT Push-Down:**
```sql
-- Original query
SELECT * FROM metrics.default.cpu ORDER BY time DESC LIMIT 100

-- Each data node returns top 100
-- Router merge-sorts and takes final top 100
```

### 4.5 Replication

#### 4.5.1 Replication Protocol

We implement a simplified Raft-based replication protocol:

```
┌────────────────────────────────────────────────────────────────────────────┐
│                      REPLICATION PROTOCOL                                   │
└────────────────────────────────────────────────────────────────────────────┘

    Primary                         Replica 1                   Replica 2
       │                               │                           │
       │  AppendEntries                │                           │
       │  (term=5, prev=100,           │                           │
       │   entries=[101,102,103])      │                           │
       │──────────────────────────────►│                           │
       │──────────────────────────────────────────────────────────►│
       │                               │                           │
       │                               │ Write to local WAL        │
       │                               │ Update match_index        │
       │                               │                           │
       │◄──────────────────────────────│                           │
       │  AppendEntriesResponse        │                           │
       │  (success=true, match=103)    │                           │
       │                               │                           │
       │◄──────────────────────────────────────────────────────────│
       │  AppendEntriesResponse        │                           │
       │  (success=true, match=103)    │                           │
       │                               │                           │
       │  Quorum reached (2/3)         │                           │
       │  Commit index = 103           │                           │
       │                               │                           │
```

#### 4.5.2 Replication State Machine

```rust
pub struct ReplicationState {
    /// Current term
    pub term: u64,
    /// Node role in this shard
    pub role: ReplicaRole,
    /// Leader of this shard (if known)
    pub leader_id: Option<NodeId>,
    /// Index of highest log entry known to be committed
    pub commit_index: u64,
    /// Index of highest log entry applied to state machine
    pub last_applied: u64,

    // Leader-only state
    /// For each follower, index of next log entry to send
    pub next_index: HashMap<NodeId, u64>,
    /// For each follower, index of highest log entry known to be replicated
    pub match_index: HashMap<NodeId, u64>,
}

pub enum ReplicaRole {
    Leader,
    Follower,
    Candidate,
}
```

#### 4.5.3 Consistency Guarantees

| Scenario | Behavior | Data Safety |
|----------|----------|-------------|
| Primary fails, quorum ACKed | New primary elected, no data loss | Safe |
| Primary fails, not quorum ACKed | Write may be lost on failover | At-risk |
| Network partition (minority) | Minority cannot accept writes | Safe |
| Network partition (majority) | Majority continues operating | Safe |

### 4.6 High Availability

#### 4.6.1 Failure Detection

```rust
pub struct FailureDetector {
    /// Heartbeat interval
    heartbeat_interval: Duration,
    /// Number of missed heartbeats before marking node as failed
    failure_threshold: u32,
    /// Phi accrual failure detector parameter
    phi_threshold: f64,
    /// Last heartbeat times
    last_heartbeats: HashMap<NodeId, Instant>,
    /// Heartbeat history for phi calculation
    heartbeat_history: HashMap<NodeId, Vec<Duration>>,
}

impl FailureDetector {
    /// Returns true if node should be considered failed
    pub fn is_failed(&self, node_id: &NodeId) -> bool {
        let last = self.last_heartbeats.get(node_id);
        match last {
            None => true,
            Some(last_time) => {
                let elapsed = last_time.elapsed();
                let threshold = self.heartbeat_interval * self.failure_threshold;
                elapsed > threshold || self.calculate_phi(node_id) > self.phi_threshold
            }
        }
    }
}
```

#### 4.6.2 Leader Election

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        LEADER ELECTION                                      │
└────────────────────────────────────────────────────────────────────────────┘

  Node 1 (Leader)        Node 2 (Follower)       Node 3 (Follower)
       │                       │                       │
       │                       │                       │
       X (crashes)             │                       │
                               │                       │
                               │  Election timeout     │
                               │  expired              │
                               │                       │
                               │  Increment term       │
                               │  Vote for self        │
                               │                       │
                               │  RequestVote          │
                               │  (term=6, last=103)   │
                               │──────────────────────►│
                               │                       │
                               │◄──────────────────────│
                               │  VoteGranted          │
                               │  (term=6)             │
                               │                       │
                               │  Received majority    │
                               │  votes (2/3)          │
                               │                       │
                               │  Become Leader        │
                               │                       │
                               │  AppendEntries        │
                               │  (heartbeat)          │
                               │──────────────────────►│
                               │                       │
```

#### 4.6.3 Failover Process

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          FAILOVER TIMELINE                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  T=0s     Primary node fails                                                │
│           │                                                                 │
│  T=3s     Heartbeat missed (1st)                                           │
│           │                                                                 │
│  T=6s     Heartbeat missed (2nd)                                           │
│           │                                                                 │
│  T=9s     Heartbeat missed (3rd) - failure threshold reached               │
│           │                                                                 │
│  T=9.1s   Node 2 starts election (random timeout: 150-300ms)               │
│           │                                                                 │
│  T=9.3s   Node 2 elected as new primary                                    │
│           │                                                                 │
│  T=9.4s   Metadata service updated with new primary                        │
│           │                                                                 │
│  T=9.5s   Router receives topology update                                   │
│           │                                                                 │
│  T=9.6s   Writes resume to new primary                                     │
│           │                                                                 │
│  Total failover time: ~10 seconds                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 4.6.4 Split-Brain Prevention

To prevent split-brain scenarios:

1. **Quorum Requirement**: Writes require majority acknowledgment
2. **Fencing Tokens**: Each leader has a monotonically increasing term
3. **Lease-Based Leadership**: Leader must renew lease periodically

```rust
pub struct LeaderLease {
    /// Current lease holder
    pub leader_id: NodeId,
    /// Lease expiration time
    pub expires_at: DateTime<Utc>,
    /// Lease duration
    pub duration: Duration,
    /// Term when lease was granted
    pub term: u64,
}

impl LeaderLease {
    pub fn is_valid(&self, current_term: u64) -> bool {
        self.term == current_term &&
        Utc::now() < self.expires_at
    }
}
```

---

## 5. Component Specifications

### 5.1 Module Structure

```
src/
├── cluster/
│   ├── mod.rs                    # Module exports
│   ├── config.rs                 # Cluster configuration
│   ├── node.rs                   # Node lifecycle management
│   │
│   ├── discovery/
│   │   ├── mod.rs
│   │   ├── static_discovery.rs   # Static seed list
│   │   ├── dns_discovery.rs      # DNS-based discovery
│   │   └── etcd_discovery.rs     # etcd-based discovery
│   │
│   ├── membership/
│   │   ├── mod.rs
│   │   ├── protocol.rs           # Membership protocol (SWIM/gossip)
│   │   ├── state.rs              # Cluster state machine
│   │   └── failure_detector.rs   # Phi accrual failure detector
│   │
│   ├── metadata/
│   │   ├── mod.rs
│   │   ├── store.rs              # Metadata storage interface
│   │   ├── etcd_store.rs         # etcd backend
│   │   ├── embedded_store.rs     # Embedded Raft backend
│   │   └── schema.rs             # Metadata schemas
│   │
│   ├── sharding/
│   │   ├── mod.rs
│   │   ├── strategy.rs           # Sharding strategies
│   │   ├── catalog_strategy.rs   # Catalog-based sharding
│   │   ├── time_strategy.rs      # Time-based sharding
│   │   ├── hash_strategy.rs      # Hash-based sharding
│   │   ├── manager.rs            # Shard assignment logic
│   │   ├── router.rs             # Routing table
│   │   └── rebalancer.rs         # Rebalancing algorithm
│   │
│   ├── replication/
│   │   ├── mod.rs
│   │   ├── wal.rs                # Write-ahead log
│   │   ├── raft.rs               # Raft consensus protocol
│   │   ├── state_machine.rs      # Replicated state machine
│   │   └── recovery.rs           # Node recovery logic
│   │
│   ├── rpc/
│   │   ├── mod.rs
│   │   ├── service.rs            # Cluster RPC service (gRPC)
│   │   ├── client.rs             # RPC client
│   │   ├── flight_ext.rs         # Arrow Flight extensions
│   │   └── messages.rs           # RPC message definitions
│   │
│   └── ha/
│       ├── mod.rs
│       ├── leader_election.rs    # Leader election
│       ├── failover.rs           # Automatic failover
│       └── fencing.rs            # Split-brain prevention
│
├── router/
│   ├── mod.rs
│   ├── write_router.rs           # Write request routing
│   ├── query_router.rs           # Query request routing
│   ├── query_planner.rs          # Distributed query planning
│   ├── result_merger.rs          # Result merging logic
│   └── load_balancer.rs          # Replica load balancing
│
└── ... (existing modules unchanged)
```

### 5.2 Key Interfaces

```rust
/// Cluster service interface
#[async_trait]
pub trait ClusterService: Send + Sync {
    /// Get current cluster state
    async fn get_cluster_state(&self) -> Result<ClusterState, ClusterError>;

    /// Get shard for a given catalog
    async fn get_shard(&self, catalog: &str) -> Result<ShardInfo, ClusterError>;

    /// Route write to appropriate node
    async fn route_write(&self, catalog: &str, data: &[u8]) -> Result<(), ClusterError>;

    /// Route query to appropriate nodes
    async fn route_query(&self, sql: &str) -> Result<RecordBatchStream, ClusterError>;

    /// Join the cluster
    async fn join(&self) -> Result<(), ClusterError>;

    /// Leave the cluster gracefully
    async fn leave(&self) -> Result<(), ClusterError>;
}

/// Replication interface
#[async_trait]
pub trait ReplicationService: Send + Sync {
    /// Append entries to WAL and replicate
    async fn append(&self, entries: Vec<WalEntry>) -> Result<u64, ReplicationError>;

    /// Handle AppendEntries RPC from leader
    async fn handle_append_entries(&self, req: AppendEntriesRequest)
        -> Result<AppendEntriesResponse, ReplicationError>;

    /// Handle RequestVote RPC for leader election
    async fn handle_request_vote(&self, req: RequestVoteRequest)
        -> Result<RequestVoteResponse, ReplicationError>;

    /// Get replication status
    async fn get_status(&self) -> ReplicationStatus;
}

/// Metadata store interface
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Get cluster topology
    async fn get_topology(&self) -> Result<ClusterTopology, MetadataError>;

    /// Update shard assignment
    async fn update_shard_assignment(&self, shard: ShardId, assignment: ShardAssignment)
        -> Result<(), MetadataError>;

    /// Watch for topology changes
    async fn watch_topology(&self) -> Result<TopologyWatcher, MetadataError>;

    /// Acquire leader lease
    async fn acquire_lease(&self, shard: ShardId, node: NodeId, duration: Duration)
        -> Result<LeaderLease, MetadataError>;
}
```

### 5.3 Data Structures

```rust
/// Cluster topology
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterTopology {
    pub version: u64,
    pub nodes: HashMap<NodeId, NodeInfo>,
    pub shards: HashMap<ShardId, ShardInfo>,
    pub routing_table: RoutingTable,
}

/// Shard information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub catalogs: Vec<String>,
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
    pub state: ShardState,
    pub created_at: DateTime<Utc>,
}

/// Routing table entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingTable {
    /// Catalog to shard mapping
    pub catalog_to_shard: HashMap<String, ShardId>,
    /// Shard to nodes mapping
    pub shard_to_nodes: HashMap<ShardId, ShardNodes>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardNodes {
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
}

/// Node capacity and load
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeCapacity {
    pub cpu_cores: u32,
    pub memory_gb: u32,
    pub disk_gb: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeLoad {
    pub cpu_percent: f32,
    pub memory_percent: f32,
    pub disk_percent: f32,
    pub writes_per_sec: f64,
    pub queries_per_sec: f64,
    pub shard_count: u32,
}
```

---

## 6. Configuration

### 6.1 Full Configuration Reference

```toml
#==============================================================================
# Stats Service Cluster Configuration
#==============================================================================

[cluster]
# Enable cluster mode
enabled = true

# Unique identifier for this node (auto-generated if not specified)
node_id = "node-1"

# Node role: "router", "data", or "hybrid"
# - router: Only routes requests, no data storage
# - data: Stores data, can be primary or replica
# - hybrid: Both router and data (default for small clusters)
role = "hybrid"

# Address this node advertises to other nodes
advertise_addr = "10.0.0.1:7000"

# Address for inter-node communication (if different from advertise_addr)
# internal_addr = "192.168.1.1:7100"

#------------------------------------------------------------------------------
# Node Discovery
#------------------------------------------------------------------------------
[cluster.discovery]
# Discovery method: "static", "dns", "etcd", "consul"
method = "static"

# Static discovery: list of seed nodes
seeds = ["10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000"]

# DNS discovery settings
# dns_name = "stats-service.cluster.local"
# dns_poll_interval_secs = 30

# etcd discovery settings
# etcd_endpoints = ["10.0.0.10:2379"]
# etcd_prefix = "/stats-service/nodes/"

#------------------------------------------------------------------------------
# Metadata Service
#------------------------------------------------------------------------------
[cluster.metadata]
# Backend: "etcd" or "embedded"
backend = "etcd"

# etcd endpoints (if backend = "etcd")
endpoints = ["10.0.0.10:2379", "10.0.0.11:2379", "10.0.0.12:2379"]

# etcd authentication (optional)
# username = "stats-service"
# password = "secret"

# etcd TLS (optional)
# ca_cert = "/etc/ssl/certs/ca.pem"
# client_cert = "/etc/ssl/certs/client.pem"
# client_key = "/etc/ssl/private/client-key.pem"

# Metadata cache TTL
cache_ttl_secs = 5

#------------------------------------------------------------------------------
# Sharding
#------------------------------------------------------------------------------
[cluster.sharding]
# Strategy: "by_catalog" (recommended), "by_time", "hash"
strategy = "by_catalog"

# Initial number of shards (for hash strategy)
# num_shards = 16

# Rebalance threshold (trigger rebalance if load imbalance exceeds this)
rebalance_threshold = 0.2

# Maximum concurrent shard moves during rebalancing
max_concurrent_moves = 2

# Minimum time between rebalance operations
rebalance_cooldown_secs = 300

#------------------------------------------------------------------------------
# Replication
#------------------------------------------------------------------------------
[cluster.replication]
# Number of replicas per shard (including primary)
factor = 3

# Replication mode: "sync", "async", "semi-sync"
# - sync: Wait for all replicas (highest durability, highest latency)
# - async: ACK immediately after local write (lowest latency, may lose data)
# - semi-sync: Wait for quorum (balanced)
mode = "semi-sync"

# Timeout for sync replication
sync_timeout_ms = 5000

# Batch size for async replication
async_batch_size = 1000

# Batch timeout for async replication
async_batch_timeout_ms = 100

#------------------------------------------------------------------------------
# Write Path
#------------------------------------------------------------------------------
[cluster.write]
# Default consistency level: "one", "quorum", "all"
consistency = "quorum"

# Write timeout
timeout_ms = 10000

# Number of retries on failure
retry_count = 3

# Backoff between retries
retry_backoff_ms = 100

#------------------------------------------------------------------------------
# Query Path
#------------------------------------------------------------------------------
[cluster.query]
# Default read consistency: "any" (any replica), "leader" (primary only)
consistency = "any"

# Query timeout
timeout_ms = 30000

# Maximum concurrent queries per node
max_concurrent_queries = 100

# Enable query result caching
enable_cache = true

# Query cache TTL
cache_ttl_secs = 60

#------------------------------------------------------------------------------
# High Availability
#------------------------------------------------------------------------------
[cluster.ha]
# Enable automatic failover
enabled = true

# Heartbeat interval
heartbeat_interval_ms = 3000

# Number of missed heartbeats before failure detection
failure_threshold = 3

# Election timeout range (randomized)
election_timeout_min_ms = 150
election_timeout_max_ms = 300

# Leader lease duration
lease_duration_secs = 10

#------------------------------------------------------------------------------
# Inter-Node Communication
#------------------------------------------------------------------------------
[cluster.rpc]
# Connection pool size per node
pool_size = 10

# Connection timeout
connect_timeout_ms = 5000

# Request timeout
request_timeout_ms = 30000

# Enable TLS for inter-node communication
tls_enabled = false

# TLS certificates (if tls_enabled = true)
# ca_cert = "/etc/ssl/certs/ca.pem"
# server_cert = "/etc/ssl/certs/server.pem"
# server_key = "/etc/ssl/private/server-key.pem"

#------------------------------------------------------------------------------
# Monitoring
#------------------------------------------------------------------------------
[cluster.monitoring]
# Enable cluster metrics
enabled = true

# Metrics export interval
export_interval_secs = 10

# Prometheus endpoint
prometheus_endpoint = "/metrics"

# Enable distributed tracing
tracing_enabled = false

# Jaeger endpoint (if tracing_enabled = true)
# jaeger_endpoint = "http://localhost:14268/api/traces"
```

### 6.2 Deployment Configurations

**Small Cluster (3 nodes, hybrid mode):**
```toml
[cluster]
enabled = true
role = "hybrid"

[cluster.replication]
factor = 3
mode = "semi-sync"

[cluster.sharding]
strategy = "by_catalog"
```

**Medium Cluster (6 nodes, separate routers):**
```toml
# Router nodes (2)
[cluster]
enabled = true
role = "router"

# Data nodes (4)
[cluster]
enabled = true
role = "data"

[cluster.replication]
factor = 2
mode = "async"
```

**Large Cluster (20+ nodes):**
```toml
[cluster]
enabled = true
role = "data"  # or "router" for dedicated routers

[cluster.sharding]
strategy = "by_catalog"
rebalance_threshold = 0.15

[cluster.replication]
factor = 3
mode = "semi-sync"

[cluster.ha]
heartbeat_interval_ms = 1000
failure_threshold = 5
```

---

## 7. API Changes

### 7.1 New Cluster Management Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cluster/status` | GET | Get cluster status and topology |
| `/cluster/nodes` | GET | List all nodes in the cluster |
| `/cluster/nodes/:id` | GET | Get specific node details |
| `/cluster/nodes/:id/decommission` | POST | Gracefully remove a node |
| `/cluster/shards` | GET | List all shards |
| `/cluster/shards/:id` | GET | Get shard details |
| `/cluster/shards/:id/move` | POST | Move shard to different node |
| `/cluster/rebalance` | POST | Trigger manual rebalance |
| `/cluster/routing` | GET | Get routing table |

### 7.2 API Response Examples

**GET /cluster/status**
```json
{
  "cluster_id": "stats-cluster-prod",
  "version": 42,
  "state": "healthy",
  "nodes": {
    "total": 6,
    "healthy": 6,
    "unhealthy": 0
  },
  "shards": {
    "total": 10,
    "healthy": 10,
    "under_replicated": 0
  },
  "leader": "node-1"
}
```

**GET /cluster/nodes**
```json
{
  "nodes": [
    {
      "node_id": "node-1",
      "advertise_addr": "10.0.0.1:7000",
      "role": "hybrid",
      "state": "active",
      "shards": ["shard-001", "shard-003"],
      "load": {
        "cpu_percent": 45.2,
        "memory_percent": 62.1,
        "disk_percent": 38.5,
        "writes_per_sec": 12500.0,
        "queries_per_sec": 850.0
      },
      "version": "1.5.0",
      "uptime_secs": 864000
    }
  ]
}
```

**GET /cluster/shards**
```json
{
  "shards": [
    {
      "shard_id": "shard-001",
      "catalogs": ["metrics_db", "logs_db"],
      "primary": "node-1",
      "replicas": ["node-2", "node-3"],
      "state": "healthy",
      "replication_lag_ms": 12,
      "size_bytes": 107374182400
    }
  ]
}
```

### 7.3 Existing API Compatibility

All existing APIs remain unchanged. The cluster layer is transparent to clients:

| API | Behavior |
|-----|----------|
| `POST /write/:db` | Router forwards to correct shard primary |
| `GET /query/:db` | Router executes distributed query if needed |
| `POST /compact/:db` | Forwarded to nodes holding the data |
| `POST /flush` | Broadcast to all data nodes |
| `GET /stats` | Aggregated from all nodes |

### 7.4 New Headers

| Header | Description | Example |
|--------|-------------|---------|
| `X-Stats-Consistency` | Override consistency level | `quorum`, `all`, `one` |
| `X-Stats-Timeout-Ms` | Override request timeout | `5000` |
| `X-Stats-Route-To` | Force routing to specific node (debug) | `node-1` |
| `X-Stats-Request-Id` | Request tracing ID | `uuid` |

---

## 8. Implementation Plan

### 8.1 Phase Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         IMPLEMENTATION PHASES                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Phase 1: Foundation                                                        │
│  ├── Cluster configuration                                                  │
│  ├── Node discovery and membership                                         │
│  ├── Metadata service integration                                          │
│  └── Basic RPC framework                                                   │
│                                                                             │
│  Phase 2: Data Distribution                                                 │
│  ├── Sharding strategies                                                   │
│  ├── Shard manager                                                         │
│  ├── Routing table                                                         │
│  └── Basic rebalancing                                                     │
│                                                                             │
│  Phase 3: Write Path                                                        │
│  ├── Write routing                                                         │
│  ├── WAL enhancements                                                      │
│  ├── Replication protocol                                                  │
│  └── Consistency levels                                                    │
│                                                                             │
│  Phase 4: Query Path                                                        │
│  ├── Query routing                                                         │
│  ├── Distributed query planning                                            │
│  ├── Result merging                                                        │
│  └── Query optimizations                                                   │
│                                                                             │
│  Phase 5: High Availability                                                 │
│  ├── Failure detection                                                     │
│  ├── Leader election                                                       │
│  ├── Automatic failover                                                    │
│  └── Split-brain prevention                                                │
│                                                                             │
│  Phase 6: Operations                                                        │
│  ├── Cluster management APIs                                               │
│  ├── Monitoring and metrics                                                │
│  ├── Rolling upgrades                                                      │
│  └── Documentation and testing                                             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 Phase 1: Foundation

**Objectives:**
- Establish cluster configuration framework
- Implement node discovery mechanisms
- Integrate metadata service (etcd)
- Create inter-node RPC framework

**Deliverables:**

| Component | Description | Priority |
|-----------|-------------|----------|
| `cluster/config.rs` | Cluster configuration parsing | P0 |
| `cluster/node.rs` | Node lifecycle (join/leave) | P0 |
| `cluster/discovery/` | Node discovery (static, DNS, etcd) | P0 |
| `cluster/metadata/` | Metadata store client | P0 |
| `cluster/rpc/` | gRPC service definitions | P0 |
| `cluster/membership/` | Membership protocol | P1 |

**Acceptance Criteria:**
- [ ] Nodes can discover each other via static config
- [ ] Nodes can register with metadata service
- [ ] Nodes can exchange heartbeats
- [ ] Cluster state visible via `/cluster/status`

### 8.3 Phase 2: Data Distribution

**Objectives:**
- Implement sharding strategies
- Create shard assignment algorithm
- Build routing table
- Implement basic rebalancing

**Deliverables:**

| Component | Description | Priority |
|-----------|-------------|----------|
| `cluster/sharding/strategy.rs` | Sharding strategy interface | P0 |
| `cluster/sharding/catalog_strategy.rs` | Catalog-based sharding | P0 |
| `cluster/sharding/manager.rs` | Shard assignment | P0 |
| `cluster/sharding/router.rs` | Routing table | P0 |
| `cluster/sharding/rebalancer.rs` | Rebalancing | P1 |
| `cluster/sharding/time_strategy.rs` | Time-based sharding | P2 |
| `cluster/sharding/hash_strategy.rs` | Hash-based sharding | P2 |

**Acceptance Criteria:**
- [ ] New catalogs automatically assigned to shards
- [ ] Routing table correctly maps catalogs to nodes
- [ ] Shard assignments visible via `/cluster/shards`
- [ ] Manual rebalance works via `/cluster/rebalance`

### 8.4 Phase 3: Write Path

**Objectives:**
- Route writes to correct shard primary
- Enhance WAL for replication
- Implement replication protocol
- Support consistency levels

**Deliverables:**

| Component | Description | Priority |
|-----------|-------------|----------|
| `router/write_router.rs` | Write request routing | P0 |
| `cluster/replication/wal.rs` | Cluster-aware WAL | P0 |
| `cluster/replication/raft.rs` | Raft consensus | P0 |
| `cluster/replication/state_machine.rs` | Replicated state | P0 |
| Consistency level support | `one`, `quorum`, `all` | P1 |

**Acceptance Criteria:**
- [ ] Writes routed to correct shard primary
- [ ] WAL entries replicated to followers
- [ ] Quorum consistency works correctly
- [ ] Write performance within 20% of single node

### 8.5 Phase 4: Query Path

**Objectives:**
- Route queries to appropriate nodes
- Implement distributed query planning
- Merge results from multiple nodes
- Optimize common query patterns

**Deliverables:**

| Component | Description | Priority |
|-----------|-------------|----------|
| `router/query_router.rs` | Query request routing | P0 |
| `router/query_planner.rs` | Distributed query planning | P0 |
| `router/result_merger.rs` | Result merging | P0 |
| Predicate push-down | Optimization | P1 |
| Partial aggregation | Optimization | P1 |
| LIMIT push-down | Optimization | P2 |

**Acceptance Criteria:**
- [ ] Single-shard queries work correctly
- [ ] Multi-shard queries return correct results
- [ ] Aggregation queries work across shards
- [ ] Query performance scales with nodes

### 8.6 Phase 5: High Availability

**Objectives:**
- Detect node failures
- Elect new leaders
- Automate failover
- Prevent split-brain

**Deliverables:**

| Component | Description | Priority |
|-----------|-------------|----------|
| `cluster/membership/failure_detector.rs` | Phi accrual detector | P0 |
| `cluster/ha/leader_election.rs` | Raft leader election | P0 |
| `cluster/ha/failover.rs` | Automatic failover | P0 |
| `cluster/ha/fencing.rs` | Split-brain prevention | P0 |
| Lease-based leadership | Enhancement | P1 |

**Acceptance Criteria:**
- [ ] Failed nodes detected within 10 seconds
- [ ] New leader elected within 5 seconds
- [ ] Writes resume within 15 seconds of failure
- [ ] No data loss during failover (quorum mode)

### 8.7 Phase 6: Operations

**Objectives:**
- Provide cluster management APIs
- Integrate monitoring and metrics
- Support rolling upgrades
- Complete documentation

**Deliverables:**

| Component | Description | Priority |
|-----------|-------------|----------|
| Cluster management APIs | Full REST API | P0 |
| Prometheus metrics | Cluster metrics | P0 |
| Rolling upgrade support | Zero-downtime upgrades | P1 |
| Grafana dashboards | Monitoring | P1 |
| Documentation | User and ops guides | P0 |
| Integration tests | End-to-end testing | P0 |

**Acceptance Criteria:**
- [ ] All management APIs functional
- [ ] Metrics exported to Prometheus
- [ ] Rolling upgrade tested
- [ ] Documentation complete

---

## 9. Testing Strategy

### 9.1 Test Categories

| Category | Description | Scope |
|----------|-------------|-------|
| Unit Tests | Individual component testing | All modules |
| Integration Tests | Component interaction testing | Cluster operations |
| System Tests | Full cluster testing | End-to-end scenarios |
| Chaos Tests | Failure injection testing | HA and recovery |
| Performance Tests | Scalability testing | Throughput/latency |

### 9.2 Test Scenarios

**Cluster Formation:**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test: Three-node cluster formation                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1. Start node-1 (becomes leader)                                           │
│ 2. Start node-2 (joins as follower)                                        │
│ 3. Start node-3 (joins as follower)                                        │
│ 4. Verify: All nodes in cluster state                                      │
│ 5. Verify: Shards distributed across nodes                                 │
│ 6. Verify: Replication factor = 3 for all shards                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Write Path:**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test: Write replication with quorum consistency                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1. Send write to router                                                    │
│ 2. Verify: Router forwards to shard primary                                │
│ 3. Verify: Primary writes to WAL                                           │
│ 4. Verify: Primary replicates to followers                                 │
│ 5. Verify: ACK returned after quorum                                       │
│ 6. Verify: Data queryable from any replica                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Failover:**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test: Automatic failover on primary failure                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1. Setup: 3-node cluster with shard primary on node-1                      │
│ 2. Action: Kill node-1 process                                             │
│ 3. Verify: Failure detected within 10 seconds                              │
│ 4. Verify: New leader elected (node-2 or node-3)                           │
│ 5. Verify: Routing table updated                                           │
│ 6. Verify: Writes succeed to new primary                                   │
│ 7. Verify: No data loss (all quorum-acked writes present)                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Network Partition:**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Test: Network partition handling                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1. Setup: 3-node cluster                                                   │
│ 2. Action: Partition node-1 from node-2 and node-3                         │
│ 3. Verify: node-2/node-3 form majority partition                           │
│ 4. Verify: node-1 cannot accept writes (no quorum)                         │
│ 5. Verify: node-2/node-3 continue operating                                │
│ 6. Action: Heal partition                                                  │
│ 7. Verify: node-1 rejoins and syncs                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 9.3 Chaos Testing

Using tools like `chaos-mesh` or custom scripts:

| Scenario | Method | Expected Outcome |
|----------|--------|------------------|
| Node crash | `kill -9` | Failover within 15s |
| Network partition | `iptables` | Majority continues |
| Disk full | `fallocate` | Graceful degradation |
| CPU stress | `stress-ng` | Latency increase, no failures |
| Memory pressure | `stress-ng --vm` | OOM handling |

### 9.4 Performance Benchmarks

| Metric | Target (3-node) | Target (6-node) |
|--------|-----------------|-----------------|
| Write throughput | 300K points/sec | 600K points/sec |
| Write latency (p99) | < 50ms | < 50ms |
| Query throughput | 1000 queries/sec | 2000 queries/sec |
| Query latency (p99) | < 500ms | < 500ms |
| Failover time | < 15 seconds | < 15 seconds |

---

## 10. Migration Path

### 10.1 Single-Node to Cluster Migration

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MIGRATION PROCEDURE                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Step 1: Prepare                                                            │
│  ├── Backup existing data                                                  │
│  ├── Install cluster-enabled version on new nodes                          │
│  └── Configure cluster settings                                            │
│                                                                             │
│  Step 2: Initialize Cluster                                                 │
│  ├── Start metadata service (etcd)                                         │
│  ├── Start first data node (becomes initial leader)                        │
│  └── Verify cluster status                                                 │
│                                                                             │
│  Step 3: Migrate Data                                                       │
│  ├── Option A: Snapshot restore                                            │
│  │   └── Copy parquet files to cluster data directory                     │
│  ├── Option B: Live migration                                              │
│  │   └── Replay WAL to cluster                                            │
│  └── Verify data integrity                                                 │
│                                                                             │
│  Step 4: Add Nodes                                                          │
│  ├── Start additional data nodes                                           │
│  ├── Wait for replication to complete                                      │
│  └── Verify shard distribution                                             │
│                                                                             │
│  Step 5: Switch Traffic                                                     │
│  ├── Update client configurations to point to cluster                      │
│  ├── Or: Start router nodes and update load balancer                       │
│  └── Monitor for errors                                                    │
│                                                                             │
│  Step 6: Decommission Old Node                                              │
│  ├── Stop old single-node instance                                         │
│  └── Archive old data (keep for rollback)                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 10.2 Configuration Migration

**Single Node (config.toml):**
```toml
[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "/data/stats"
```

**Cluster Node (config.toml):**
```toml
[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "/data/stats"

# New cluster section
[cluster]
enabled = true
node_id = "node-1"
role = "hybrid"
advertise_addr = "10.0.0.1:7000"

[cluster.discovery]
method = "static"
seeds = ["10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000"]

[cluster.metadata]
backend = "etcd"
endpoints = ["10.0.0.10:2379"]

[cluster.replication]
factor = 3
mode = "semi-sync"
```

### 10.3 Rollback Procedure

If migration fails:

1. Stop cluster nodes
2. Restore single-node configuration
3. Restore data from backup
4. Restart single-node service
5. Update client configurations

---

## 11. Operational Considerations

### 11.1 Deployment Topologies

**Small (3 nodes):**
```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   Node 1    │  │   Node 2    │  │   Node 3    │
│   Hybrid    │  │   Hybrid    │  │   Hybrid    │
│ Router+Data │  │ Router+Data │  │ Router+Data │
└─────────────┘  └─────────────┘  └─────────────┘
```

**Medium (6 nodes):**
```
┌─────────────┐  ┌─────────────┐
│   Router 1  │  │   Router 2  │
└──────┬──────┘  └──────┬──────┘
       │                │
       └────────┬───────┘
                │
┌───────────────┼───────────────┐
│               │               │
▼               ▼               ▼
┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
│ Data 1  │ │ Data 2  │ │ Data 3  │ │ Data 4  │
└─────────┘ └─────────┘ └─────────┘ └─────────┘
```

**Large (20+ nodes):**
```
                    Load Balancer
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   ┌────▼────┐      ┌────▼────┐      ┌────▼────┐
   │ Router  │      │ Router  │      │ Router  │
   │  Pool   │      │  Pool   │      │  Pool   │
   └────┬────┘      └────┬────┘      └────┬────┘
        │                │                │
        └────────────────┼────────────────┘
                         │
    ┌────────────────────┼────────────────────┐
    │                    │                    │
    ▼                    ▼                    ▼
┌────────┐          ┌────────┐          ┌────────┐
│ Data   │          │ Data   │          │ Data   │
│ Zone A │          │ Zone B │          │ Zone C │
│ (6-8)  │          │ (6-8)  │          │ (6-8)  │
└────────┘          └────────┘          └────────┘
```

### 11.2 Monitoring

**Key Metrics:**

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `cluster_nodes_total` | Total nodes in cluster | Change |
| `cluster_nodes_healthy` | Healthy nodes | < expected |
| `cluster_shards_total` | Total shards | Change |
| `cluster_shards_under_replicated` | Under-replicated shards | > 0 |
| `cluster_replication_lag_ms` | Replication lag | > 1000 |
| `cluster_leader_elections_total` | Leader elections | Spike |
| `cluster_write_latency_p99` | Write latency | > 100ms |
| `cluster_query_latency_p99` | Query latency | > 1000ms |

**Grafana Dashboard:**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Stats Service Cluster Dashboard                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │ Cluster Health  │  │ Nodes: 6/6      │  │ Shards: 10      │             │
│  │     ● OK        │  │ Healthy         │  │ Replicated      │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │ Write Throughput (points/sec)                               │           │
│  │  500K ─┬────────────────────────────────────────────        │           │
│  │        │    ___/\___/\___/\___/\___                         │           │
│  │  250K ─┼───/                                                │           │
│  │        │                                                    │           │
│  │      0 ─┴────────────────────────────────────────────       │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │ Replication Lag (ms)                                        │           │
│  │   100 ─┬────────────────────────────────────────────        │           │
│  │        │                                                    │           │
│  │    50 ─┼───────────────────────────────────────────         │           │
│  │        │_______________________________________________     │           │
│  │      0 ─┴────────────────────────────────────────────       │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 11.3 Capacity Planning

**Per-Node Requirements:**

| Workload | CPU | Memory | Disk | Network |
|----------|-----|--------|------|---------|
| Light | 4 cores | 16 GB | 500 GB SSD | 1 Gbps |
| Medium | 8 cores | 32 GB | 2 TB SSD | 10 Gbps |
| Heavy | 16 cores | 64 GB | 8 TB SSD | 25 Gbps |

**Sizing Formula:**
```
nodes_required = max(
    ceil(total_write_throughput / write_throughput_per_node),
    ceil(total_storage / storage_per_node),
    ceil(total_queries / queries_per_node),
    replication_factor  # Minimum for HA
)
```

### 11.4 Backup and Recovery

**Backup Strategy:**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          BACKUP STRATEGY                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Metadata Backup (etcd)                                                  │
│     └── etcdctl snapshot save backup.db                                    │
│     └── Frequency: Hourly                                                  │
│                                                                             │
│  2. WAL Archival                                                            │
│     └── Ship WAL segments to object storage (S3/GCS)                       │
│     └── Frequency: Continuous                                              │
│                                                                             │
│  3. Parquet Snapshot                                                        │
│     └── Copy parquet files to backup location                              │
│     └── Frequency: Daily                                                   │
│                                                                             │
│  Recovery Options:                                                          │
│  ├── Point-in-time: Restore snapshot + replay WAL                          │
│  ├── Latest: Restore latest snapshot                                       │
│  └── Partial: Restore specific catalogs/shards                             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 12. Risk Assessment

### 12.1 Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Data loss during failover | Low | High | Quorum writes, WAL replication |
| Split-brain | Low | High | Fencing tokens, quorum requirement |
| Performance degradation | Medium | Medium | Benchmarking, capacity planning |
| Replication lag | Medium | Low | Monitoring, async with bounded lag |
| Complex debugging | High | Medium | Distributed tracing, logging |

### 12.2 Operational Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Misconfiguration | Medium | High | Validation, defaults, documentation |
| Upgrade failures | Medium | Medium | Rolling upgrades, rollback plan |
| Resource exhaustion | Medium | Medium | Monitoring, auto-scaling |
| etcd failure | Low | High | etcd cluster with 3-5 nodes |

### 12.3 Risk Mitigation Plan

**Data Loss Prevention:**
1. Default to quorum consistency
2. WAL-based replication
3. Regular backups to object storage
4. Point-in-time recovery capability

**Split-Brain Prevention:**
1. Quorum requirement for writes
2. Fencing tokens for leadership
3. Lease-based leader validity
4. Network partition handling

---

## 13. Appendix

### 13.1 Glossary

| Term | Definition |
|------|------------|
| **Catalog** | Database namespace (maps to InfluxDB database) |
| **Shard** | Unit of data distribution and replication |
| **Primary** | Shard replica that accepts writes |
| **Replica** | Copy of a shard for redundancy |
| **Router** | Stateless node that routes requests |
| **Data Node** | Node that stores shard data |
| **WAL** | Write-Ahead Log for durability |
| **Term** | Raft consensus term number |
| **Quorum** | Majority of replicas (N/2 + 1) |

### 13.2 References

- [Raft Consensus Algorithm](https://raft.github.io/)
- [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
- [DataFusion](https://github.com/apache/arrow-datafusion)
- [etcd](https://etcd.io/)
- [InfluxDB Clustering](https://docs.influxdata.com/enterprise_influxdb/)
- [TiKV Architecture](https://tikv.org/docs/)
- [CockroachDB Architecture](https://www.cockroachlabs.com/docs/)

### 13.3 Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-13 | Stats Service Team | Initial proposal |

---

## Approval

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Technical Lead | | | |
| Engineering Manager | | | |
| Product Owner | | | |

---

*This document is a living proposal and will be updated as the design evolves during implementation.*
