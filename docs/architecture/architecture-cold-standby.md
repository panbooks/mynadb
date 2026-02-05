# Cold Standby Architecture

## Overview

MynaDB implements a **Cold Standby Architecture** for high availability that prevents the "avalanche failure" problem common in Kafka-style ISR (In-Sync Replica) designs.

## Problem Statement: Avalanche Risk in ISR Model

### The Traditional ISR Approach

In a traditional Kafka-like ISR model, every node acts as both a leader for some partitions and a standby for others:

```
TRADITIONAL ISR MODEL (PROBLEMATIC):
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  ┌─────────────────────┐          ┌─────────────────────┐              │
│  │      data-1         │          │      data-2         │              │
│  │                     │          │                     │              │
│  │  Leader: part-A     │◄────────►│  Leader: part-B     │              │
│  │  Standby: part-B    │  sync    │  Standby: part-A    │              │
│  │                     │          │                     │              │
│  │  Load: 50%          │          │  Load: 50%          │              │
│  └─────────────────────┘          └─────────────────────┘              │
│                                                                         │
│  Normal Operation: Each node handles ~50% of traffic                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### When a Node Fails

```
AFTER data-1 CRASHES:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  ┌─────────────────────┐          ┌─────────────────────┐              │
│  │      data-1         │          │      data-2         │              │
│  │                     │          │                     │              │
│  │    ╳ CRASHED ╳      │          │  Leader: part-A  ◄─┼── NEW!       │
│  │                     │          │  Leader: part-B     │              │
│  │                     │          │                     │              │
│  │                     │          │  Load: 100% !!!     │              │
│  └─────────────────────┘          └─────────────────────┘              │
│                                                                         │
│  PROBLEM: data-2 now handles ALL traffic (2x load)                      │
│                                                                         │
│  For Kafka: Acceptable (consumers do light sequential reads)            │
│  For MynaDB: DANGEROUS (heavy SQL queries, aggregations, joins)         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### The Avalanche Effect

```
AVALANCHE FAILURE PROGRESSION:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Time T+0: data-1 crashes                                               │
│       │                                                                 │
│       ▼                                                                 │
│  Time T+1: data-2 takes over (100% load)                               │
│       │                                                                 │
│       ▼                                                                 │
│  Time T+2: data-2 becomes overloaded                                   │
│       │    - Query latency increases 10x                               │
│       │    - Memory pressure triggers GC pauses                        │
│       │    - CPU saturated with aggregations                           │
│       ▼                                                                 │
│  Time T+3: data-2 starts timing out                                    │
│       │    - Health checks fail                                        │
│       │    - Clients see errors                                        │
│       ▼                                                                 │
│  Time T+4: data-2 crashes (OOM or timeout)                             │
│       │                                                                 │
│       ▼                                                                 │
│  Time T+5: TOTAL CLUSTER FAILURE                                       │
│            No nodes available to serve traffic                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Solution: Cold Standby Architecture

### Core Principles

1. **Dedicated Standby Nodes**: Standby nodes are "cold" - they never serve queries
2. **Complete Takeover**: On failover, Standby becomes Primary (no extra load on others)
3. **Global Ratio Config**: Simple configuration (e.g., 3 Primaries : 1 Standby)
4. **Avalanche Prevention**: No cascading load increases

### Architecture Diagram

```
COLD STANDBY ARCHITECTURE:
┌─────────────────────────────────────────────────────────────────────────┐
│                           Meta Cluster (Raft)                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                     │
│  │   meta-1    │──│   meta-2    │──│   meta-3    │                     │
│  │  (Leader)   │  │ (Follower)  │  │ (Follower)  │                     │
│  └──────┬──────┘  └─────────────┘  └─────────────┘                     │
│         │                                                               │
│         │ Node Registration / Mode Assignment                           │
│         ▼                                                               │
├─────────────────────────────────────────────────────────────────────────┤
│                         Data Node Cluster                               │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      PRIMARY NODES                               │   │
│  │         (Actively serve writes and queries)                      │   │
│  │                                                                  │   │
│  │  ┌───────────┐   ┌───────────┐   ┌───────────┐                  │   │
│  │  │  data-0   │   │  data-1   │   │  data-2   │                  │   │
│  │  │ (Primary) │   │ (Primary) │   │ (Primary) │                  │   │
│  │  │           │   │           │   │           │                  │   │
│  │  │ part-A    │   │ part-B    │   │ part-C    │                  │   │
│  │  │ part-D    │   │ part-E    │   │ part-F    │                  │   │
│  │  └─────┬─────┘   └─────┬─────┘   └─────┬─────┘                  │   │
│  │        │               │               │                         │   │
│  └────────┼───────────────┼───────────────┼─────────────────────────┘   │
│           │               │               │                             │
│           │  Replication  │  Replication  │  Replication                │
│           │    Stream     │    Stream     │    Stream                   │
│           ▼               ▼               ▼                             │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      STANDBY NODES                               │   │
│  │         (Cold backup - only receive replication)                 │   │
│  │                                                                  │   │
│  │  ┌───────────────────────────────────────────────────────────┐  │   │
│  │  │                       data-3                               │  │   │
│  │  │                      (Standby)                             │  │   │
│  │  │                                                            │  │   │
│  │  │  Replicated: part-A, part-B, part-C, part-D, part-E, part-F│  │   │
│  │  │                                                            │  │   │
│  │  │  Status: COLD (no queries, no writes, just receiving data) │  │   │
│  │  └───────────────────────────────────────────────────────────┘  │   │
│  │                                                                  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Ratio: 3:1 (3 Primaries : 1 Standby)                                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Node Mode State Machine

```
DATA NODE MODE STATE MACHINE:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│                        ┌─────────────────┐                              │
│                        │   Node Joins    │                              │
│                        │    Cluster      │                              │
│                        └────────┬────────┘                              │
│                                 │                                       │
│                                 ▼                                       │
│                  ┌──────────────────────────────┐                       │
│                  │     Meta Auto-Assigns Mode   │                       │
│                  │                              │                       │
│                  │  if standby_count < target:  │                       │
│                  │      mode = Standby          │                       │
│                  │  else:                       │                       │
│                  │      mode = Primary          │                       │
│                  └──────────────┬───────────────┘                       │
│                                 │                                       │
│              ┌──────────────────┴──────────────────┐                    │
│              ▼                                     ▼                    │
│     ┌─────────────────┐                   ┌─────────────────┐          │
│     │                 │                   │                 │          │
│     │    PRIMARY      │                   │    STANDBY      │          │
│     │                 │                   │                 │          │
│     │  - Serve writes │                   │  - Receive      │          │
│     │  - Serve queries│                   │    replication  │          │
│     │  - Flush to     │                   │  - NO queries   │          │
│     │    Parquet      │                   │  - NO writes    │          │
│     │  - Leader for   │                   │  - Warm cache   │          │
│     │    partitions   │                   │    ready        │          │
│     │                 │                   │                 │          │
│     └────────┬────────┘                   └────────┬────────┘          │
│              │                                     │                    │
│              │  Primary Fails                      │                    │
│              │  (lease expires)                    │                    │
│              ▼                                     │                    │
│     ┌─────────────────┐                           │                    │
│     │  Meta Detects   │                           │                    │
│     │    Failure      │───────────────────────────┤                    │
│     └─────────────────┘     Promote Standby       │                    │
│                                                   ▼                    │
│                                          ┌─────────────────┐           │
│                                          │   PROMOTED TO   │           │
│                                          │    PRIMARY      │           │
│                                          │                 │           │
│                                          │  - Takes over   │           │
│                                          │    partitions   │           │
│                                          │  - Starts       │           │
│                                          │    serving      │           │
│                                          └─────────────────┘           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Component Interactions

### 1. Node Registration Flow

```
NODE REGISTRATION SEQUENCE:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│   Data Node                    Meta Leader                Router        │
│       │                            │                         │          │
│       │   1. RegisterNode          │                         │          │
│       │   (role=Data)              │                         │          │
│       │ ──────────────────────────►│                         │          │
│       │                            │                         │          │
│       │                   2. Count current nodes             │          │
│       │                      Primary: 3                      │          │
│       │                      Standby: 0                      │          │
│       │                      Target:  1 (ratio 3:1)          │          │
│       │                            │                         │          │
│       │                   3. Assign mode = Standby           │          │
│       │                      (standby_count < target)        │          │
│       │                            │                         │          │
│       │   4. Response              │                         │          │
│       │   (lease_id, mode=Standby) │                         │          │
│       │ ◄──────────────────────────│                         │          │
│       │                            │                         │          │
│       │                            │  5. Publish Event       │          │
│       │                            │  NodeJoined(mode=Standby)          │
│       │                            │ ────────────────────────►          │
│       │                            │                         │          │
│       │                            │                  6. Update cache   │
│       │                            │                  (exclude from     │
│       │                            │                   query routing)   │
│       │                            │                         │          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2. Write Flow (Normal Operation)

```
WRITE FLOW (PRIMARY NODES ONLY):
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Client          Router           Primary Node      Standby Node        │
│    │                │                   │                │              │
│    │  1. Write      │                   │                │              │
│    │  (InfluxDB LP) │                   │                │              │
│    │ ──────────────►│                   │                │              │
│    │                │                   │                │              │
│    │                │  2. Route to      │                │              │
│    │                │  partition leader │                │              │
│    │                │  (PRIMARY only)   │                │              │
│    │                │ ─────────────────►│                │              │
│    │                │                   │                │              │
│    │                │                   │  3. Write to   │              │
│    │                │                   │     memtable   │              │
│    │                │                   │                │              │
│    │                │                   │  4. Replicate  │              │
│    │                │                   │ ──────────────►│              │
│    │                │                   │                │              │
│    │                │                   │                │  5. Store    │
│    │                │                   │                │  in memtable │
│    │                │                   │                │  (NO flush)  │
│    │                │                   │                │              │
│    │                │                   │  6. ACK        │              │
│    │                │                   │ ◄──────────────│              │
│    │                │                   │                │              │
│    │                │  7. Success       │                │              │
│    │                │ ◄─────────────────│                │              │
│    │                │                   │                │              │
│    │  8. Success    │                   │                │              │
│    │ ◄──────────────│                   │                │              │
│    │                │                   │                │              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3. Query Flow (Primary Nodes Only)

```
QUERY FLOW:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Client          Router           Primary Node      Standby Node        │
│    │                │                   │                │              │
│    │  1. SQL Query  │                   │                │              │
│    │ ──────────────►│                   │                │              │
│    │                │                   │                │              │
│    │                │  2. Get active    │                │              │
│    │                │  PRIMARY nodes    │                │              │
│    │                │  (filter mode)    │                │              │
│    │                │                   │                │              │
│    │                │  3. Route query   │                │              │
│    │                │ ─────────────────►│                │              │
│    │                │                   │                │              │
│    │                │                   │  4. Execute    │              │
│    │                │                   │  via DataFusion│              │
│    │                │                   │                │              │
│    │                │  5. Results       │                │              │
│    │                │ ◄─────────────────│                │              │
│    │                │                   │                │              │
│    │  6. Results    │                   │                │              │
│    │ ◄──────────────│                   │                │              │
│    │                │                   │                │              │
│    │                │        ╳ NEVER ROUTES TO STANDBY ╳ │              │
│    │                │                   │                │              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4. Failover Flow

```
FAILOVER SEQUENCE:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Time    Meta Leader           data-1 (Primary)    data-3 (Standby)    │
│    │          │                      │                   │              │
│    │          │  Heartbeat OK        │                   │              │
│  T+0    ◄─────│◄─────────────────────│                   │              │
│    │          │                      │                   │              │
│    │          │                      │  CRASH!           │              │
│  T+1          │                      ╳                   │              │
│    │          │                                          │              │
│    │          │  No heartbeat...                         │              │
│  T+2          │                                          │              │
│    │          │                                          │              │
│    │          │  Lease expires                           │              │
│  T+3    ──────│────►                                     │              │
│    │          │                                          │              │
│    │          │  1. Detect failure                       │              │
│    │          │  2. Select Standby to promote            │              │
│    │          │  3. Change mode: Standby → Primary       │              │
│    │          │  4. Transfer partition leadership        │              │
│    │          │                                          │              │
│    │          │  5. Publish DataNodeModeChanged          │              │
│  T+4    ──────│─────────────────────────────────────────►│              │
│    │          │                                          │              │
│    │          │                             6. Receive   │              │
│    │          │                                event     │              │
│    │          │                                          │              │
│    │          │                             7. Switch to │              │
│    │          │                                PRIMARY   │              │
│    │          │                                mode      │              │
│    │          │                                          │              │
│    │          │                             8. Start     │              │
│    │          │                                serving   │              │
│    │          │                                queries   │              │
│  T+5          │                                          │              │
│    │          │                                          │              │
│    │    Router receives event, updates cache             │              │
│    │    New queries route to promoted node               │              │
│    │                                                     │              │
│                                                                         │
│  TOTAL FAILOVER TIME: ~5 seconds (configurable via lease TTL)          │
│  NO LOAD INCREASE ON OTHER PRIMARY NODES                               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Replica Assignment

### Without Cold Standby (Legacy Mode)

```
LEGACY MODE (all nodes can be leaders or standbys):
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Available Nodes: [data-0, data-1, data-2, data-3]                     │
│  Replication Factor: 2                                                  │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Partition: mydb/default/cpu/date=2024-01-15                    │   │
│  │                                                                  │   │
│  │  Leader:   data-1  (selected by load balancing)                 │   │
│  │  Standby:  data-2  (selected from remaining nodes)              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Problem: data-1 is ALSO a standby for other partitions               │
│           If data-0 fails, data-1 gets extra load                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### With Cold Standby (New Mode)

```
COLD STANDBY MODE (separate pools):
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  PRIMARY Pool:  [data-0, data-1, data-2]  (serve traffic)              │
│  STANDBY Pool:  [data-3]                   (cold backup)               │
│  Replication Factor: 2                                                  │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Partition: mydb/default/cpu/date=2024-01-15                    │   │
│  │                                                                  │   │
│  │  Leader:   data-1  (from PRIMARY pool)                          │   │
│  │  Standby:  data-3  (from STANDBY pool)                          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Partition: mydb/default/mem/date=2024-01-15                    │   │
│  │                                                                  │   │
│  │  Leader:   data-2  (from PRIMARY pool)                          │   │
│  │  Standby:  data-3  (from STANDBY pool)                          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Benefit: data-3 is DEDICATED standby                                  │
│           No Primary node takes on extra load during failover          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Zone-Aware Assignment

```
ZONE-AWARE COLD STANDBY:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Zone A                          Zone B                                 │
│  ┌────────────────────┐          ┌────────────────────┐                │
│  │                    │          │                    │                │
│  │  data-0 (Primary)  │          │  data-2 (Primary)  │                │
│  │  data-1 (Primary)  │          │  data-3 (Standby)  │                │
│  │                    │          │                    │                │
│  └────────────────────┘          └────────────────────┘                │
│                                                                         │
│  Partition Assignment:                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Leader: data-0 (Zone A)                                        │   │
│  │  Standby: data-3 (Zone B)  ← Different zone for HA             │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  If Zone A fails:                                                       │
│    → data-3 (Zone B) promoted to Primary                               │
│    → Service continues from Zone B                                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Comparison: ISR vs Cold Standby

| Aspect | ISR Model | Cold Standby |
|--------|-----------|--------------|
| **Node Roles** | Any node = Leader + Standby | Primary OR Standby (not both) |
| **Query Serving** | All nodes serve queries | Only Primary nodes |
| **On Failover** | Existing node gets extra load | Standby becomes Primary |
| **Avalanche Risk** | HIGH (doubled load) | NONE (dedicated takeover) |
| **Resource Usage** | All nodes actively used | Standbys idle until needed |
| **Memory Efficiency** | Good (all nodes serve) | Lower (standbys idle) |
| **Failover Speed** | Fast (data already hot) | Fast (data replicated) |
| **Operational Model** | Complex (mixed roles) | Simple (clear separation) |

---

## Configuration Examples

### Small Cluster (Development)

```
4 Nodes, Ratio 3:1
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐           │
│  │  data-0   │  │  data-1   │  │  data-2   │  │  data-3   │           │
│  │ (Primary) │  │ (Primary) │  │ (Primary) │  │ (Standby) │           │
│  └───────────┘  └───────────┘  └───────────┘  └───────────┘           │
│                                                                         │
│  Capacity: 3 Primary nodes serving traffic                             │
│  Fault Tolerance: 1 Primary failure (Standby takes over)               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Medium Cluster (Production)

```
8 Nodes, Ratio 3:1
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  PRIMARY POOL (6 nodes):                                               │
│  ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐         │
│  │data-0 │ │data-1 │ │data-2 │ │data-3 │ │data-4 │ │data-5 │         │
│  └───────┘ └───────┘ └───────┘ └───────┘ └───────┘ └───────┘         │
│                                                                         │
│  STANDBY POOL (2 nodes):                                               │
│  ┌───────┐ ┌───────┐                                                   │
│  │data-6 │ │data-7 │                                                   │
│  └───────┘ └───────┘                                                   │
│                                                                         │
│  Capacity: 6 Primary nodes serving traffic                             │
│  Fault Tolerance: 2 Primary failures simultaneously                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Large Cluster (High Availability)

```
20 Nodes, Ratio 4:1
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  PRIMARY POOL (16 nodes):                                              │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ data-0  data-1  data-2  data-3  data-4  data-5  data-6  data-7  │   │
│  │ data-8  data-9  data-10 data-11 data-12 data-13 data-14 data-15 │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  STANDBY POOL (4 nodes):                                               │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ data-16  data-17  data-18  data-19                              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Capacity: 16 Primary nodes serving traffic                            │
│  Fault Tolerance: 4 Primary failures simultaneously                    │
│  Efficiency: 80% of nodes actively serving (16/20)                     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Event Flow

### Events Published by Meta

```
EVENT TYPES FOR COLD STANDBY:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  1. NodeJoined (existing)                                              │
│     ┌─────────────────────────────────────────────────────────────┐    │
│     │ {                                                           │    │
│     │   "node_id": "data-3",                                      │    │
│     │   "grpc_addr": "data-3.mynadb:9001",                        │    │
│     │   "mode": "standby"  ← NEW FIELD                           │    │
│     │ }                                                           │    │
│     └─────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  2. DataNodeModeChanged (NEW)                                          │
│     ┌─────────────────────────────────────────────────────────────┐    │
│     │ {                                                           │    │
│     │   "node_id": "data-3",                                      │    │
│     │   "old_mode": "standby",                                    │    │
│     │   "new_mode": "primary",                                    │    │
│     │   "reason": "failover",                                     │    │
│     │   "failed_node_id": "data-1"                                │    │
│     │ }                                                           │    │
│     └─────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  3. ReplicaAssigned (existing, updated)                                │
│     ┌─────────────────────────────────────────────────────────────┐    │
│     │ {                                                           │    │
│     │   "partition_key": "mydb/default/cpu/date=2024-01-15",      │    │
│     │   "leader_node_id": "data-1",      ← From PRIMARY pool     │    │
│     │   "standby_node_ids": ["data-3"]   ← From STANDBY pool     │    │
│     │ }                                                           │    │
│     └─────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Kubernetes Integration

### StatefulSet Configuration

```yaml
# Single StatefulSet - Meta auto-assigns modes
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mynadb-data
spec:
  replicas: 8  # Meta assigns: 6 Primary, 2 Standby (ratio 3:1)
  selector:
    matchLabels:
      app: mynadb-data
  template:
    spec:
      containers:
      - name: mynadb
        image: mynadb:latest
        env:
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: META_ENDPOINTS
          value: "meta-0.mynadb:9000,meta-1.mynadb:9000,meta-2.mynadb:9000"
```

### Horizontal Pod Autoscaler

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: mynadb-data-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: mynadb-data
  minReplicas: 4   # Minimum: 3 Primary + 1 Standby
  maxReplicas: 20  # Scale up to 15 Primary + 5 Standby
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

---

## Failure Scenarios

### Scenario 1: Single Primary Failure

```
SINGLE PRIMARY FAILURE:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Before:                                                                │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                       │
│  │ data-0  │ │ data-1  │ │ data-2  │ │ data-3  │                       │
│  │ Primary │ │ Primary │ │ Primary │ │ Standby │                       │
│  │ Load:33%│ │ Load:33%│ │ Load:33%│ │ Load:0% │                       │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘                       │
│                                                                         │
│  Event: data-1 crashes                                                  │
│                                                                         │
│  After:                                                                 │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                       │
│  │ data-0  │ │ data-1  │ │ data-2  │ │ data-3  │                       │
│  │ Primary │ │ CRASHED │ │ Primary │ │ PRIMARY │ ← Promoted!          │
│  │ Load:33%│ │         │ │ Load:33%│ │ Load:33%│                       │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘                       │
│                                                                         │
│  Result: NO load increase on data-0 or data-2                          │
│          data-3 takes over data-1's partitions                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Scenario 2: Multiple Primary Failures

```
MULTIPLE PRIMARY FAILURES (with 2 Standbys):
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Before:                                                                │
│  ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐          │
│  │data-0 │ │data-1 │ │data-2 │ │data-3 │ │data-4 │ │data-5 │          │
│  │Primary│ │Primary│ │Primary│ │Primary│ │Standby│ │Standby│          │
│  └───────┘ └───────┘ └───────┘ └───────┘ └───────┘ └───────┘          │
│                                                                         │
│  Event: data-1 and data-2 crash simultaneously                         │
│                                                                         │
│  After:                                                                 │
│  ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐          │
│  │data-0 │ │data-1 │ │data-2 │ │data-3 │ │data-4 │ │data-5 │          │
│  │Primary│ │CRASHED│ │CRASHED│ │Primary│ │PRIMARY│ │PRIMARY│          │
│  │       │ │       │ │       │ │       │ │       │ │       │          │
│  └───────┘ └───────┘ └───────┘ └───────┘ └───────┘ └───────┘          │
│                                                                         │
│  Result: Both standbys promoted                                        │
│          System continues with 4 active nodes                          │
│          NO avalanche (each node takes 25% load)                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Scenario 3: Standby Exhaustion

```
STANDBY EXHAUSTION:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  State: All standbys already promoted, another Primary fails           │
│                                                                         │
│  ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐                              │
│  │data-0 │ │data-1 │ │data-2 │ │data-3 │                              │
│  │CRASHED│ │Primary│ │Primary│ │Primary│ (was Standby)                │
│  └───────┘ └───────┘ └───────┘ └───────┘                              │
│                                                                         │
│  If data-1 also crashes:                                               │
│  - No Standby available                                                 │
│  - Partitions remain unassigned until:                                 │
│    1. K8s autoscaler adds new pod (assigned as Standby, then promoted) │
│    2. Manual intervention                                               │
│    3. data-1 recovers and rejoins                                      │
│                                                                         │
│  Mitigation: Configure higher Standby ratio for critical workloads     │
│              Use HPA to auto-scale on node count                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Performance Characteristics

### Resource Utilization

```
RESOURCE UTILIZATION COMPARISON:

ISR Model (4 nodes, all active):
┌─────────────────────────────────────────────────────────────────────────┐
│  CPU:    ████████████████████████████████ 100% utilized                │
│  Memory: ████████████████████████████████ 100% utilized                │
│  Nodes:  4 serving / 4 total = 100% efficiency                         │
│  Risk:   HIGH (any failure doubles load on others)                     │
└─────────────────────────────────────────────────────────────────────────┘

Cold Standby (4 nodes, 3:1 ratio):
┌─────────────────────────────────────────────────────────────────────────┐
│  CPU:    ████████████████████████         75% utilized                 │
│  Memory: ████████████████████████████     87% utilized (replication)   │
│  Nodes:  3 serving / 4 total = 75% efficiency                          │
│  Risk:   LOW (dedicated failover capacity)                             │
└─────────────────────────────────────────────────────────────────────────┘
```

### Failover Timing

```
FAILOVER TIMING BREAKDOWN:

┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Component              Duration    Notes                               │
│  ─────────────────────────────────────────────────────────────────     │
│  Heartbeat interval     3s          Configurable                        │
│  Lease TTL              10s         Configurable (default)              │
│  Detection delay        ~10s        Lease expiration                    │
│  Mode change            <100ms      Raft commit                         │
│  Event propagation      <100ms      To routers                          │
│  Cache update           <10ms       Router local                        │
│  ─────────────────────────────────────────────────────────────────     │
│  TOTAL                  ~10-11s     Worst case                          │
│                                                                         │
│  With aggressive settings (lease TTL=5s):                              │
│  TOTAL                  ~5-6s       Production recommended              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Files

| File | Purpose |
|------|---------|
| `src/meta/types.rs` | `DataNodeMode` enum, `NodeInfo.mode` field |
| `src/meta/config.rs` | `StandbyConfig` struct |
| `src/meta/raft/state_machine.rs` | Auto-assign mode, promotion logic |
| `src/meta/replica_assigner.rs` | Separate Primary/Standby pools |
| `src/meta/service/grpc.rs` | gRPC handlers for mode management |
| `src/meta/service/meta_node.rs` | Public API for mode operations |
| `src/router/routing/shard_cache.rs` | Filter to Primary nodes |
| `src/router/routing/cache_invalidation.rs` | Handle mode change events |
| `src/node/data.rs` | Mode-aware startup |
| `proto/meta.proto` | Protocol buffer definitions |

---

## See Also

- [Guide: Cold Standby Operations](guide-cold-standby.md) - Configuration and operational guide
- [Architecture: Cluster Mode](architecture-cluster.md) - Overall cluster architecture
- [Architecture: Replication](architecture-replication.md) - Data replication details
