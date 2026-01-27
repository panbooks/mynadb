# Guide: Warm Standby Replication

This guide covers how to configure and operate warm standby replication for zero-downtime failover.

## Quick Start

### 1. Enable Replication on Data Nodes

Edit each data node's configuration:

```toml
# data-node-1.toml
[data_node]
node_id = "data-1"
advertise_addr = "192.168.1.10:7200"

[data_node.replication]
enabled = true
mode = "async"
batch_interval_ms = 100
```

```toml
# data-node-2.toml (standby)
[data_node]
node_id = "data-2"
advertise_addr = "192.168.1.11:7200"

[data_node.replication]
enabled = true
mode = "async"
batch_interval_ms = 100
```

### 2. Assign Partition Replicas via Meta API

```bash
# Assign data-1 as leader, data-2 as standby for a partition
curl -X POST http://meta-node:7100/api/v1/replicas \
  -H "Content-Type: application/json" \
  -d '{
    "partition_key": "mydb/default/cpu/date=2024-01-15",
    "leader_node_id": "data-1",
    "standby_node_ids": ["data-2"],
    "replication_factor": 2
  }'
```

### 3. Verify Replication Status

```bash
# Check replica status
curl http://meta-node:7100/api/v1/replicas/mydb/default/cpu/date=2024-01-15

# Expected response:
{
  "partition_key": "mydb/default/cpu/date=2024-01-15",
  "leader_node_id": "data-1",
  "standby_node_ids": ["data-2"],
  "state": "InSync",
  "replication_lag_ms": 50
}
```

## Configuration Reference

### Data Node Configuration

```toml
[data_node.replication]
# Enable/disable replication
enabled = true

# Replication mode:
# - "async": ACK immediately after local write (default, lowest latency)
# - "sync": ACK after at least one standby confirms
# - "sync_all": ACK after all standbys confirm (highest durability)
mode = "async"

# How often to batch and send replication entries (milliseconds)
# Lower = less lag, higher network overhead
# Higher = more lag, better throughput
batch_interval_ms = 100

# Maximum entries per replication batch
# Larger batches are more efficient but use more memory
max_batch_size = 1000

# Timeout waiting for standby acknowledgment (milliseconds)
# Only relevant for sync modes
ack_timeout_ms = 5000

# Alert threshold for replication lag (milliseconds)
# Logs warning if any standby exceeds this
max_lag_alert_ms = 10000
```

### Meta Node Configuration

```toml
[meta.failover]
# Node lease TTL - how long before a node is considered dead
# Shorter = faster detection, more false positives
# Longer = slower detection, fewer false positives
lease_ttl_secs = 10

# Automatically promote standby when leader fails
auto_promote = true
```

## Operations

### Viewing Replica Assignments

```bash
# List all replicas
curl http://meta-node:7100/api/v1/replicas

# Get specific partition
curl http://meta-node:7100/api/v1/replicas/mydb/default/cpu/date=2024-01-15

# Get all replicas for a leader
curl http://meta-node:7100/api/v1/replicas?leader=data-1
```

### Manual Failover

Promote a standby to leader manually:

```bash
curl -X POST http://meta-node:7100/api/v1/replicas/mydb/default/cpu/date=2024-01-15/promote
```

### Adding a Standby

Add a new standby to an existing partition:

```bash
curl -X PUT http://meta-node:7100/api/v1/replicas/mydb/default/cpu/date=2024-01-15 \
  -H "Content-Type: application/json" \
  -d '{
    "standby_node_ids": ["data-2", "data-3"]
  }'
```

### Removing a Standby

```bash
curl -X PUT http://meta-node:7100/api/v1/replicas/mydb/default/cpu/date=2024-01-15 \
  -H "Content-Type: application/json" \
  -d '{
    "standby_node_ids": ["data-2"]
  }'
```

### Checking Replication Health

```bash
# Get replication stats from a data node
curl http://data-node:7200/api/v1/replication/stats

# Expected response:
{
  "stream_count": 5,
  "avg_lag_ms": 45,
  "max_lag_ms": 120,
  "in_sync_count": 5
}
```

## Failover Scenarios

### Scenario 1: Leader Node Crashes

**What happens:**

1. Router detects write failure (~100ms)
2. Router marks leader as Suspect
3. Router routes to standby immediately
4. Writes continue with minimal interruption
5. Meta detects via lease expiry (~10s)
6. Meta officially promotes standby
7. New leader starts flushing to Parquet

**User impact:** ~150ms write latency spike, no data loss

### Scenario 2: Leader Node Graceful Shutdown

**Recommended procedure:**

```bash
# 1. Drain the leader (stop new partition assignments)
curl -X POST http://data-1:7200/api/v1/drain

# 2. Wait for replication to catch up
curl http://data-1:7200/api/v1/replication/stats
# Wait until all standbys show replication_lag_ms < 100

# 3. Manually promote standbys
curl -X POST http://meta-node:7100/api/v1/replicas/promote-all?leader=data-1

# 4. Shutdown the node
curl -X POST http://data-1:7200/api/v1/shutdown
```

### Scenario 3: Network Partition

If leader can't reach standbys but Router can reach both:

1. Replication to standby fails
2. Leader continues accepting writes (async mode)
3. Replication lag increases
4. Alert triggered at max_lag_alert_ms
5. When network recovers, standby catches up

**For sync mode:** Writes block until standby reachable or timeout

### Scenario 4: Standby Falls Behind

If standby replication lag exceeds threshold:

```bash
# Check which standbys are behind
curl http://meta-node:7100/api/v1/replicas?state=CatchingUp

# Force a standby to resync from scratch
curl -X POST http://data-2:7200/api/v1/replication/resync \
  -d '{"partition_key": "mydb/default/cpu/date=2024-01-15"}'
```

## Monitoring

### Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `replication_lag_ms` | Time since last standby ACK | > 10s |
| `replication_queue_size` | Pending entries to send | > 10000 |
| `standby_state` | InSync/CatchingUp/Offline | Offline |
| `failover_count` | Number of failovers | Any (investigate) |

### Log Messages

**Normal operation:**
```
INFO  Started replication stream to data-2 for partition mydb/default/cpu/date=2024-01-15
DEBUG Flushed 500 rows to standby data-2 in 45ms
```

**Failover:**
```
WARN  Leader data-1 unhealthy, instant failover to standby data-2 for partition mydb/default/cpu/date=2024-01-15
INFO  Auto-failover: Node data-1 was leader for partition mydb/default/cpu/date=2024-01-15, promoting standby
INFO  Promoted partition leader: mydb/default/cpu/date=2024-01-15 -> new_leader=data-2
```

**Problems:**
```
WARN  Replication DoPut failed: connection refused
WARN  Sequence gap detected for partition: expected 1001 but got 1005
WARN  Standby cache full: current=4000MB, batch=100KB, max=4096MB
```

## Best Practices

### 1. Replication Factor

| Use Case | RF | Standbys | Notes |
|----------|-----|----------|-------|
| Development | 1 | 0 | No replication |
| Production (standard) | 2 | 1 | Survives 1 failure |
| Production (critical) | 3 | 2 | Survives 2 failures |

### 2. Node Placement

Spread leader and standbys across:
- Different physical machines
- Different racks (if possible)
- Different availability zones (for cloud)

### 3. Memory Sizing

Standbys need similar memory as leaders:
- MemTable size is replicated
- Plan for 2x memory per partition with RF=2

### 4. Network Bandwidth

Estimate replication bandwidth:
```
Bandwidth = Write Rate × Replication Factor

Example:
- 100 MB/s writes
- RF=2 (1 standby)
- Network need: 100 MB/s between leader and standby
```

### 5. Batch Interval Tuning

| Scenario | batch_interval_ms | Trade-off |
|----------|-------------------|-----------|
| Low latency | 10-50 | More network overhead |
| Balanced | 100 | Default, good for most |
| High throughput | 200-500 | Higher lag acceptable |

## Troubleshooting

### Standby Not Receiving Data

1. Check network connectivity:
   ```bash
   curl http://standby:7200/health
   ```

2. Check replication stream status on leader:
   ```bash
   curl http://leader:7200/api/v1/replication/streams
   ```

3. Check standby logs for errors:
   ```bash
   grep "replication\|REPLICATE" /var/log/mynadb/data-2.log
   ```

### High Replication Lag

1. Check network latency between nodes:
   ```bash
   ping leader-ip
   ```

2. Check if standby is overloaded:
   ```bash
   curl http://standby:7200/api/v1/stats
   ```

3. Increase batch size for better throughput:
   ```toml
   max_batch_size = 2000
   ```

### Failover Not Happening

1. Check Meta lease expiry:
   ```bash
   curl http://meta-node:7100/api/v1/leases
   ```

2. Check if auto_promote is enabled:
   ```bash
   curl http://meta-node:7100/api/v1/config
   ```

3. Manually trigger promotion:
   ```bash
   curl -X POST http://meta-node:7100/api/v1/replicas/.../promote
   ```

### Data Inconsistency After Failover

If standby was behind when promoted:

1. Check for sequence gaps in logs
2. The gap data is lost (was in leader's memtable)
3. To minimize: use `mode = "sync"` for critical data

## Example: 3-Node Cluster Setup

### Node Configuration

```toml
# data-1.toml (will be leader for some partitions)
[data_node]
node_id = "data-1"
advertise_addr = "10.0.1.10:7200"
meta_endpoints = ["10.0.1.1:7100", "10.0.1.2:7100", "10.0.1.3:7100"]

[data_node.replication]
enabled = true
mode = "async"

# data-2.toml
[data_node]
node_id = "data-2"
advertise_addr = "10.0.1.11:7200"
meta_endpoints = ["10.0.1.1:7100", "10.0.1.2:7100", "10.0.1.3:7100"]

[data_node.replication]
enabled = true
mode = "async"

# data-3.toml
[data_node]
node_id = "data-3"
advertise_addr = "10.0.1.12:7200"
meta_endpoints = ["10.0.1.1:7100", "10.0.1.2:7100", "10.0.1.3:7100"]

[data_node.replication]
enabled = true
mode = "async"
```

### Initial Replica Assignment

Distribute partitions across nodes:

```bash
# Partition 1: data-1 leads, data-2 standby
curl -X POST http://meta:7100/api/v1/replicas -d '{
  "partition_key": "mydb/default/cpu/date=2024-01-15",
  "leader_node_id": "data-1",
  "standby_node_ids": ["data-2"],
  "replication_factor": 2
}'

# Partition 2: data-2 leads, data-3 standby
curl -X POST http://meta:7100/api/v1/replicas -d '{
  "partition_key": "mydb/default/memory/date=2024-01-15",
  "leader_node_id": "data-2",
  "standby_node_ids": ["data-3"],
  "replication_factor": 2
}'

# Partition 3: data-3 leads, data-1 standby
curl -X POST http://meta:7100/api/v1/replicas -d '{
  "partition_key": "mydb/default/disk/date=2024-01-15",
  "leader_node_id": "data-3",
  "standby_node_ids": ["data-1"],
  "replication_factor": 2
}'
```

This creates a balanced cluster where each node is leader for some partitions and standby for others.
