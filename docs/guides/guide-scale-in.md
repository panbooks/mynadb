# Scale-In Operations Guide

This guide covers the configuration, deployment, monitoring, and troubleshooting of MynaDB's Scale-In feature for reducing cluster size.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Configuration Reference](#configuration-reference)
3. [Operations](#operations)
4. [Monitoring](#monitoring)
5. [Troubleshooting](#troubleshooting)
6. [Best Practices](#best-practices)
7. [FAQ](#faq)

---

## Quick Start

### Prerequisites

- MynaDB cluster with Object Store backend (S3/MinIO/OSS)
- Meta service running with HTTP API enabled
- Minimum 2 active Primary nodes (to maintain availability)

### Enable Meta HTTP API

The scale-in endpoints are exposed via HTTP. Ensure Meta has `http_port` configured:

```toml
[meta]
node_id = 1
bind_addr = "0.0.0.0:7100"
advertise_addr = "192.168.1.10:7100"
http_port = 9000    # Scale-in API port
```

### Retire a Single Node

```bash
# Start retirement
curl -X POST http://meta-0:9000/nodes/data-5/retire

# Check status
curl http://meta-0:9000/nodes/data-5/retire/status

# Wait for "drained" state, then stop the node
```

### Retire Multiple Nodes

```bash
# Get recommendations
curl "http://meta-0:9000/cluster/scale-in/recommend?primaries=3"

# Execute batch retirement
curl -X POST http://meta-0:9000/cluster/scale-in/execute \
  -H "Content-Type: application/json" \
  -d '{"nodes": ["data-5", "data-12", "data-17"]}'
```

---

## Configuration Reference

### Meta Configuration

```toml
[meta]
# Unique node ID in Raft cluster
node_id = 1

# gRPC address for Raft communication
bind_addr = "0.0.0.0:7100"
advertise_addr = "192.168.1.10:7100"

# HTTP API port for scale-in and management
http_port = 9000

# Data directory
data_dir = "/data/meta"
```

### Related Standby Configuration

Scale-in works with the Cold Standby architecture:

```toml
[meta.standby]
enabled = true
primary_standby_ratio = 3    # 3 Primaries per 1 Standby
auto_assign_mode = true
auto_promote = true
min_primary_nodes = 2
```

---

## Operations

### Retiring a Primary Node

#### Step 1: Initiate Retirement

```bash
curl -X POST http://meta-0:9000/nodes/data-5/retire
```

Response:
```json
{
  "success": true,
  "status": "retiring",
  "node_id": "data-5",
  "partitions_to_reassign": 47
}
```

#### Step 2: Monitor Progress

```bash
curl http://meta-0:9000/nodes/data-5/retire/status
```

Response:
```json
{
  "node_id": "data-5",
  "state": "retiring",
  "is_retiring": true,
  "started_at": 1706444400000,
  "partitions_total": 47,
  "partitions_reassigned": 32,
  "memtable_rows_remaining": 125000,
  "progress_percent": 68
}
```

#### Step 3: Wait for Drained State

```bash
# Poll until state = "drained"
while [ "$(curl -s http://meta-0:9000/nodes/data-5/retire/status | jq -r .state)" != "drained" ]; do
    echo "Waiting for data-5 to drain..."
    sleep 10
done
echo "data-5 is drained, safe to stop"
```

#### Step 4: Stop the Node

```bash
# SSH to the node and stop
ssh data-5 "mynadb data stop"

# Or via systemd
ssh data-5 "systemctl stop mynadb"
```

### Retiring a Standby Node

Standby retirement is faster since they don't own unique data:

```bash
# Initiate retirement
curl -X POST http://meta-0:9000/nodes/data-standby-3/retire

# Response shows no partitions to reassign
{
  "success": true,
  "status": "retiring",
  "node_id": "data-standby-3",
  "partitions_to_reassign": 0
}

# Standby becomes drained almost immediately
# Stop the node
ssh data-standby-3 "mynadb data stop"
```

### Canceling a Retirement

If you need to abort a retirement in progress:

```bash
curl -X POST http://meta-0:9000/nodes/data-5/retire/cancel
```

Response:
```json
{
  "success": true,
  "status": "active",
  "node_id": "data-5"
}
```

**Note:** Cancellation restores the node to Active state, but partitions that were already reassigned stay with their new leaders.

### Getting Scale-In Recommendations

Ask Meta to recommend which nodes to retire:

```bash
# Recommend 5 primaries and 3 standbys to retire
curl "http://meta-0:9000/cluster/scale-in/recommend?primaries=5&standbys=3"
```

Response:
```json
{
  "recommended_primaries": ["data-15", "data-23", "data-47", "data-52", "data-61"],
  "recommended_standbys": ["data-78", "data-82", "data-91"],
  "reasons": {
    "data-15": "lowest_partition_count (23 partitions)",
    "data-23": "lowest_partition_count (25 partitions)",
    "data-47": "lowest_partition_count (27 partitions)",
    "data-52": "lowest_partition_count (29 partitions)",
    "data-61": "lowest_partition_count (31 partitions)",
    "data-78": "standby with high redundancy",
    "data-82": "standby with high redundancy",
    "data-91": "standby with high redundancy"
  },
  "warnings": []
}
```

### Batch Scale-In

Retire multiple nodes in one operation:

```bash
curl -X POST http://meta-0:9000/cluster/scale-in/execute \
  -H "Content-Type: application/json" \
  -d '{
    "nodes": ["data-15", "data-23", "data-78", "data-82"],
    "force_flush": true
  }'
```

Response:
```json
{
  "success": true,
  "nodes_retired": ["data-15", "data-23", "data-78", "data-82"],
  "nodes_failed": [],
  "errors": {}
}
```

### Force Flush During Retirement

To speed up retirement, you can force a memtable flush:

```bash
# Via Meta API
curl -X POST http://meta-0:9000/nodes/data-5/retire \
  -H "Content-Type: application/json" \
  -d '{"force_flush": true}'

# Or directly on the Data node
curl -X POST http://data-5:7000/retire \
  -H "Content-Type: application/json" \
  -d '{"force_flush": true}'
```

---

## Monitoring

### Key Metrics to Watch

#### During Scale-In

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `retiring_nodes_count` | Number of nodes in Retiring state | > 20% of cluster |
| `memtable_rows_remaining` | Unflushed rows on retiring node | Stuck for > 30 min |
| `partitions_reassigned` | Progress of partition migration | Not increasing |
| `write_latency_p99` | Write latency during migration | > 2x baseline |

#### Cluster Health

```bash
# Check all node states
curl http://router:8080/nodes | jq '.[] | {id: .node_id, state: .state}'

# Check partition distribution
curl http://meta-0:9000/api/v1/partitions/stats
```

### Monitoring Script

```bash
#!/bin/bash
# monitor-scale-in.sh

NODE_ID=$1
META_HOST=${2:-meta-0:9000}

while true; do
    STATUS=$(curl -s "http://$META_HOST/nodes/$NODE_ID/retire/status")

    STATE=$(echo $STATUS | jq -r .state)
    PROGRESS=$(echo $STATUS | jq -r .progress_percent)
    ROWS=$(echo $STATUS | jq -r .memtable_rows_remaining)

    echo "[$(date)] $NODE_ID: state=$STATE progress=$PROGRESS% rows=$ROWS"

    if [ "$STATE" = "drained" ]; then
        echo "Node $NODE_ID is drained. Safe to stop."
        break
    fi

    sleep 10
done
```

Usage:
```bash
./monitor-scale-in.sh data-5 meta-0:9000
```

### Grafana Dashboard Queries

```promql
# Nodes by state
count by (state) (mynadb_node_state)

# Retiring nodes memtable size
mynadb_memtable_rows{state="retiring"}

# Partition reassignment rate
rate(mynadb_partitions_reassigned_total[5m])
```

---

## Troubleshooting

### Common Issues

#### Node Stuck in Retiring State

**Symptoms:**
- Node remains in "retiring" state for > 30 minutes
- `memtable_rows_remaining` not decreasing

**Causes:**
1. Memtable flush is slow or stuck
2. S3 connectivity issues
3. Disk I/O saturation

**Solutions:**

```bash
# Check flush status on the data node
curl http://data-5:7000/stats

# Force a flush
curl -X POST http://data-5:7000/flush

# Check S3 connectivity
curl http://data-5:7000/health

# Check for errors in logs
ssh data-5 "tail -100 /var/log/mynadb/data.log | grep -i error"
```

#### Partition Reassignment Failing

**Symptoms:**
- `partitions_reassigned` not increasing
- Error messages about "no available nodes"

**Causes:**
1. Not enough active Primary nodes
2. All potential targets are also retiring
3. Network issues between Meta and Data nodes

**Solutions:**

```bash
# Check active Primary count
curl http://meta-0:9000/api/v1/nodes | jq '[.nodes[] | select(.mode == "primary" and .state == "active")] | length'

# Ensure at least 2 active Primaries remain
# If too many nodes retiring, cancel some:
curl -X POST http://meta-0:9000/nodes/data-5/retire/cancel
```

#### Scale-In Recommendation Empty

**Symptoms:**
- `recommend` endpoint returns empty arrays

**Causes:**
1. Cluster too small to safely scale in
2. All nodes have critical partitions

**Solutions:**

```bash
# Check warnings in response
curl "http://meta-0:9000/cluster/scale-in/recommend?primaries=1" | jq .warnings

# Common warnings:
# - "Cannot remove node: would leave partitions without leader"
# - "Minimum Primary node count would be violated"
```

#### Data Node Not Responding to Retire

**Symptoms:**
- `/retire` returns 503 or connection refused

**Causes:**
1. Node not running
2. Meta client not configured on Data node

**Solutions:**

```bash
# Check if node is running
ssh data-5 "ps aux | grep mynadb"

# Check Data node logs
ssh data-5 "tail -50 /var/log/mynadb/data.log"

# Verify Meta client is configured
ssh data-5 "cat /etc/mynadb/config.toml | grep -A5 '\[cluster\]'"
```

### Recovery Procedures

#### Retiring Node Crashed Before Drain

If a retiring node crashes before reaching "drained" state:

1. **Data is safe** - Flushed data is in S3, unflushed data is on Standbys
2. Meta will detect the crash and mark node as Inactive
3. Standby will be promoted to handle the partitions
4. No manual intervention needed

```bash
# Verify partition coverage after crash
curl http://meta-0:9000/api/v1/partitions/health
```

#### Rollback After Partial Scale-In

If you need to restore capacity after starting scale-in:

```bash
# 1. Cancel any in-progress retirements
for node in data-15 data-23 data-47; do
    curl -X POST "http://meta-0:9000/nodes/$node/retire/cancel"
done

# 2. Restart any stopped nodes
for node in data-15 data-23 data-47; do
    ssh $node "systemctl start mynadb"
done

# 3. Wait for nodes to rejoin
sleep 30

# 4. Verify cluster state
curl http://meta-0:9000/api/v1/nodes | jq '.nodes[] | {id: .node_id, state: .state}'
```

---

## Best Practices

### Planning Scale-In

1. **Schedule during low-traffic periods**
   - Memtable flush is faster with fewer writes
   - Less impact on query latency

2. **Scale in gradually**
   - Remove 10-20% of nodes at a time
   - Wait for complete drain before next batch

3. **Use recommendations**
   - Let Meta choose optimal nodes
   - Review warnings before executing

4. **Monitor throughout**
   - Watch partition distribution
   - Check write/query latency

### Sizing Guidelines

| Cluster Size | Max Concurrent Retires | Drain Time (typical) |
|--------------|----------------------|----------------------|
| 10 nodes | 1-2 | 5-10 min |
| 50 nodes | 5-10 | 5-15 min |
| 100 nodes | 10-20 | 10-20 min |
| 500 nodes | 50-100 | 15-30 min |

### Pre-Scale-In Checklist

- [ ] Verify Object Store connectivity from all nodes
- [ ] Check current memtable sizes (smaller = faster drain)
- [ ] Ensure minimum 2 active Primaries will remain
- [ ] Review partition distribution for hotspots
- [ ] Notify on-call team of planned maintenance
- [ ] Prepare rollback commands

### Post-Scale-In Verification

```bash
# 1. Verify all retired nodes are stopped
for node in data-15 data-23; do
    ssh $node "pgrep mynadb" || echo "$node: stopped ✓"
done

# 2. Check partition health
curl http://meta-0:9000/api/v1/partitions/health

# 3. Verify no orphaned data
curl http://meta-0:9000/api/v1/partitions/orphaned

# 4. Check write path
echo "cpu,host=test value=42.0" | curl -X POST http://router:8080/write/testdb --data-binary @-

# 5. Check read path
curl "http://router:8080/query/testdb?sql=SELECT+*+FROM+cpu+LIMIT+1"
```

---

## FAQ

### Q: Can I retire a node that's currently receiving writes?

**A:** Yes. When you retire a node, the Router immediately stops sending new writes to it. In-flight writes will complete, then all new writes go to other nodes. The retiring node's memtable is flushed to S3.

### Q: What happens to queries during scale-in?

**A:** Queries continue to work. Retiring nodes can still serve reads (they may have unflushed memtable data). Once drained, other nodes read the retired node's data directly from S3.

### Q: How long does scale-in take?

**A:** Typically 5-20 minutes per node, depending on:
- Memtable size (more data = longer flush)
- S3 write speed
- Number of partitions to reassign

### Q: Can I speed up scale-in?

**A:** Yes:
1. Use `force_flush: true` to trigger immediate flush
2. Scale in during low-traffic periods (smaller memtables)
3. Pre-flush memtables before starting retirement

### Q: What if I retire too many nodes?

**A:** Cancel retirements that haven't drained yet:
```bash
curl -X POST http://meta-0:9000/nodes/data-5/retire/cancel
```

For already-stopped nodes, restart them and they'll rejoin as new nodes.

### Q: Is data lost during scale-in?

**A:** No. The scale-in process ensures:
1. All memtable data is flushed to S3 before transitioning to Drained
2. Retiring nodes continue serving reads until data is in S3
3. Other nodes can query retired node's S3 data

### Q: Can I retire a Meta node?

**A:** This guide covers Data node scale-in. Meta node removal requires Raft membership changes, which is a different process. See `guide-meta-operations.md`.

### Q: Do I need to clean up S3 data after scale-in?

**A:** No. Data written by retired nodes remains in S3 under their prefix (e.g., `s3://bucket/data-15/...`). Active nodes query all prefixes automatically. You can optionally compact old prefixes during maintenance windows.

---

## API Reference

### Meta HTTP Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/nodes/{node_id}/retire` | POST | Start node retirement |
| `/nodes/{node_id}/retire/status` | GET | Get retirement progress |
| `/nodes/{node_id}/retire/cancel` | POST | Cancel retirement |
| `/cluster/scale-in/recommend` | GET | Get recommendations |
| `/cluster/scale-in/execute` | POST | Batch retire nodes |

### Request/Response Examples

See [Architecture Documentation](architecture-scale-in.md) for detailed API specifications.

---

## Related Documentation

- [Architecture: Scale-In](architecture-scale-in.md) - Technical architecture
- [Architecture: Cold Standby](architecture-cold-standby.md) - Standby node design
- [Guide: Cold Standby](guide-cold-standby.md) - Standby operations
- [Architecture: Cluster](architecture-cluster-v2.md) - Overall cluster design
