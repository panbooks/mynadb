# Cold Standby Operations Guide

This guide covers the configuration, deployment, monitoring, and troubleshooting of MynaDB's Cold Standby Architecture.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Configuration Reference](#configuration-reference)
3. [Deployment Scenarios](#deployment-scenarios)
4. [Monitoring](#monitoring)
5. [Operations](#operations)
6. [Troubleshooting](#troubleshooting)
7. [Best Practices](#best-practices)

---

## Quick Start

### Enable Cold Standby

Add to your `config.toml`:

```toml
[cluster.standby]
enabled = true
primary_standby_ratio = 3    # 3 Primaries per 1 Standby
auto_assign_mode = true      # Meta auto-assigns modes
auto_promote = true          # Auto-promote on failure
min_primary_nodes = 2        # Minimum Primaries before assigning Standbys
```

### Verify Configuration

```bash
# Check node modes after startup
curl http://meta-1:9000/api/v1/nodes | jq '.nodes[] | {id: .node_id, mode: .mode}'
```

Expected output:
```json
{"id": "data-0", "mode": "primary"}
{"id": "data-1", "mode": "primary"}
{"id": "data-2", "mode": "primary"}
{"id": "data-3", "mode": "standby"}
```

---

## Configuration Reference

### StandbyConfig

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `false` | Enable Cold Standby architecture |
| `primary_standby_ratio` | u32 | `3` | Number of Primaries per Standby |
| `auto_assign_mode` | bool | `true` | Auto-assign mode on node registration |
| `auto_promote` | bool | `true` | Auto-promote Standby on Primary failure |
| `min_primary_nodes` | usize | `2` | Minimum Primaries before assigning Standbys |

### Configuration Examples

#### Development (4 nodes)

```toml
[cluster.standby]
enabled = true
primary_standby_ratio = 3    # 3 Primary, 1 Standby
auto_assign_mode = true
auto_promote = true
min_primary_nodes = 1        # Allow Standby assignment sooner
```

#### Production (12 nodes)

```toml
[cluster.standby]
enabled = true
primary_standby_ratio = 3    # 9 Primary, 3 Standby
auto_assign_mode = true
auto_promote = true
min_primary_nodes = 3        # Ensure stable Primary pool first
```

#### High Availability (20 nodes)

```toml
[cluster.standby]
enabled = true
primary_standby_ratio = 4    # 16 Primary, 4 Standby
auto_assign_mode = true
auto_promote = true
min_primary_nodes = 4
```

### Lease Configuration (Faster Failover)

```toml
[cluster]
heartbeat_interval_ms = 3000   # 3 seconds
lease_ttl_ms = 10000           # 10 seconds (detection time)

# For faster failover (aggressive):
# heartbeat_interval_ms = 1000
# lease_ttl_ms = 5000
```

---

## Deployment Scenarios

### Scenario 1: New Cluster Deployment

```
STARTUP SEQUENCE:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Step 1: Start Meta cluster (3 nodes)                                   │
│  ────────────────────────────────────                                   │
│  $ docker-compose up -d meta-1 meta-2 meta-3                           │
│                                                                         │
│  Wait for Raft leader election (~5-10s)                                │
│                                                                         │
│  Step 2: Start Data nodes                                               │
│  ────────────────────────────                                           │
│  $ docker-compose up -d data-0 data-1 data-2 data-3                    │
│                                                                         │
│  Registration order and mode assignment:                                │
│    1. data-0 joins → Primary (no nodes yet)                            │
│    2. data-1 joins → Primary (1 Primary, need more)                    │
│    3. data-2 joins → Primary (2 Primaries, min_primary_nodes met)      │
│    4. data-3 joins → Standby (3 Primaries, target_standby=1)           │
│                                                                         │
│  Step 3: Verify cluster state                                           │
│  ────────────────────────────                                           │
│  $ curl http://meta-1:9000/api/v1/nodes                                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Scenario 2: Adding Nodes to Existing Cluster

```bash
# Current state: 3 Primary, 1 Standby (ratio 3:1)
# Adding 2 more nodes

# Start new nodes
docker-compose up -d data-4 data-5

# Expected assignment:
# data-4 → Primary (4 Primary, target_standby=1, current=1, OK)
# data-5 → Primary (5 Primary, target_standby=1, current=1, OK)

# Verify
curl http://meta-1:9000/api/v1/nodes | jq '.nodes[] | {id: .node_id, mode: .mode}'
```

### Scenario 3: Scaling for Higher Redundancy

```bash
# Current: 6 Primary, 2 Standby (ratio 3:1)
# Want: Higher redundancy with ratio 2:1

# Update config on all Meta nodes:
# [cluster.standby]
# primary_standby_ratio = 2

# Restart Meta cluster (rolling restart OK)
docker-compose restart meta-1 meta-2 meta-3

# Add more nodes to become Standbys
docker-compose up -d data-8 data-9

# Expected: 6 Primary, 3 Standby (new ratio 2:1)
```

---

## Monitoring

### Key Metrics

#### Node Mode Distribution

```bash
# Check mode distribution
curl -s http://meta-1:9000/api/v1/nodes | jq '
  .nodes | group_by(.mode) |
  map({mode: .[0].mode, count: length})
'
```

Output:
```json
[
  {"mode": "primary", "count": 6},
  {"mode": "standby", "count": 2}
]
```

#### Health Check Script

```bash
#!/bin/bash
# health_check.sh

META_ENDPOINT="http://meta-1:9000"

# Get node counts
NODES=$(curl -s "$META_ENDPOINT/api/v1/nodes")
PRIMARY_COUNT=$(echo "$NODES" | jq '[.nodes[] | select(.mode == "primary")] | length')
STANDBY_COUNT=$(echo "$NODES" | jq '[.nodes[] | select(.mode == "standby")] | length')
TOTAL_COUNT=$(echo "$NODES" | jq '.nodes | length')

echo "=== Cold Standby Status ==="
echo "Primary nodes:  $PRIMARY_COUNT"
echo "Standby nodes:  $STANDBY_COUNT"
echo "Total nodes:    $TOTAL_COUNT"
echo "Ratio:          $PRIMARY_COUNT:$STANDBY_COUNT"

# Check if ratio is maintained
EXPECTED_STANDBY=$((PRIMARY_COUNT / 3))  # Assuming ratio 3:1
if [ "$STANDBY_COUNT" -lt "$EXPECTED_STANDBY" ]; then
    echo "WARNING: Insufficient standbys! Expected $EXPECTED_STANDBY, have $STANDBY_COUNT"
    exit 1
fi

echo "Status: HEALTHY"
```

#### Prometheus Metrics

```yaml
# prometheus.yml scrape config
scrape_configs:
  - job_name: 'mynadb-meta'
    static_configs:
      - targets: ['meta-1:9000', 'meta-2:9000', 'meta-3:9000']
    metrics_path: /metrics
```

Key metrics to alert on:

```promql
# Alert: No Standby nodes available
mynadb_data_nodes_by_mode{mode="standby"} == 0

# Alert: Standby ratio below target
mynadb_data_nodes_by_mode{mode="primary"} / mynadb_data_nodes_by_mode{mode="standby"} > 4

# Alert: Recent promotion (potential issue)
increase(mynadb_standby_promotions_total[5m]) > 0
```

#### Grafana Dashboard

```json
{
  "title": "Cold Standby Status",
  "panels": [
    {
      "title": "Node Distribution",
      "type": "piechart",
      "targets": [
        {"expr": "mynadb_data_nodes_by_mode", "legendFormat": "{{mode}}"}
      ]
    },
    {
      "title": "Standby Promotions",
      "type": "graph",
      "targets": [
        {"expr": "rate(mynadb_standby_promotions_total[1m])", "legendFormat": "promotions/min"}
      ]
    },
    {
      "title": "Failover Events",
      "type": "table",
      "targets": [
        {"expr": "mynadb_failover_events", "format": "table"}
      ]
    }
  ]
}
```

### Event Monitoring

Subscribe to events for real-time monitoring:

```bash
# Watch for mode changes
curl -N "http://meta-1:9000/api/v1/events/subscribe?topic=nodes" | \
  jq --unbuffered 'select(.event_type == "DataNodeModeChanged")'
```

Output on failover:
```json
{
  "sequence": 1234,
  "event_type": "DataNodeModeChanged",
  "topic": "nodes",
  "payload": {
    "node_id": "data-3",
    "old_mode": "standby",
    "new_mode": "primary",
    "reason": "failover",
    "failed_node_id": "data-1"
  },
  "timestamp": 1706400000000
}
```

---

## Operations

### Manual Promotion

Promote a Standby node manually (for maintenance or testing):

```bash
# Promote data-3 from Standby to Primary
curl -X POST http://meta-1:9000/api/v1/nodes/data-3/promote \
  -H "Content-Type: application/json" \
  -d '{"reason": "planned_maintenance"}'
```

Response:
```json
{
  "success": true,
  "promoted_node_id": "data-3",
  "old_mode": "standby",
  "new_mode": "primary"
}
```

### Check Node Mode

```bash
# Get specific node's mode
curl http://meta-1:9000/api/v1/nodes/data-3/mode
```

Response:
```json
{
  "node_id": "data-3",
  "mode": "primary",
  "assigned_at": "2024-01-15T10:30:00Z"
}
```

### Planned Maintenance

```
MAINTENANCE PROCEDURE:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Goal: Take data-1 (Primary) offline for maintenance                   │
│                                                                         │
│  Step 1: Verify Standby availability                                    │
│  ─────────────────────────────────────                                  │
│  $ curl http://meta-1:9000/api/v1/nodes | \                            │
│      jq '.nodes[] | select(.mode == "standby")'                        │
│                                                                         │
│  Ensure at least 1 Standby is available                                │
│                                                                         │
│  Step 2: Drain the Primary node (optional)                             │
│  ─────────────────────────────────────────                              │
│  $ curl -X POST http://meta-1:9000/api/v1/nodes/data-1/drain           │
│                                                                         │
│  This moves partition leadership away before shutdown                   │
│                                                                         │
│  Step 3: Stop the node                                                  │
│  ─────────────────────────                                              │
│  $ docker stop data-1                                                   │
│                                                                         │
│  Step 4: Meta detects failure, promotes Standby                         │
│  ───────────────────────────────────────────────                        │
│  (Automatic if auto_promote=true)                                       │
│                                                                         │
│  Step 5: Perform maintenance on data-1                                  │
│  ────────────────────────────────────────                               │
│                                                                         │
│  Step 6: Restart data-1                                                 │
│  ───────────────────────                                                │
│  $ docker start data-1                                                  │
│                                                                         │
│  Node rejoins as Standby (based on current ratio)                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Rolling Upgrade

```bash
#!/bin/bash
# rolling_upgrade.sh

NODES="data-0 data-1 data-2 data-3"
NEW_IMAGE="mynadb:v2.0.0"

for node in $NODES; do
    echo "=== Upgrading $node ==="

    # Get current mode
    MODE=$(curl -s http://meta-1:9000/api/v1/nodes/$node/mode | jq -r '.mode')
    echo "Current mode: $MODE"

    if [ "$MODE" == "primary" ]; then
        # Check if Standby is available for failover
        STANDBY_COUNT=$(curl -s http://meta-1:9000/api/v1/nodes | \
            jq '[.nodes[] | select(.mode == "standby" and .state == "active")] | length')

        if [ "$STANDBY_COUNT" -eq 0 ]; then
            echo "ERROR: No Standby available! Aborting upgrade."
            exit 1
        fi
    fi

    # Stop node
    docker stop $node

    # Wait for failover (if Primary)
    if [ "$MODE" == "primary" ]; then
        echo "Waiting for failover..."
        sleep 15
    fi

    # Upgrade and restart
    docker pull $NEW_IMAGE
    docker run -d --name $node $NEW_IMAGE

    # Wait for node to rejoin
    echo "Waiting for $node to rejoin..."
    sleep 10

    # Verify node is healthy
    STATE=$(curl -s http://meta-1:9000/api/v1/nodes/$node | jq -r '.state')
    if [ "$STATE" != "active" ]; then
        echo "WARNING: $node state is $STATE"
    fi

    echo "$node upgraded successfully"
    echo ""
done

echo "=== Rolling upgrade complete ==="
```

---

## Troubleshooting

### Problem: No Standby Assigned

**Symptoms:**
- All nodes are Primary
- Warning in Meta logs: "No standby nodes available"

**Diagnosis:**
```bash
# Check current node count vs min_primary_nodes
curl http://meta-1:9000/api/v1/nodes | jq '.nodes | length'

# Check config
grep min_primary_nodes /etc/mynadb/config.toml
```

**Solutions:**
1. Add more nodes (need > min_primary_nodes for Standby assignment)
2. Reduce `min_primary_nodes` in config
3. Manually set a node to Standby mode

### Problem: Standby Not Promoted on Failure

**Symptoms:**
- Primary crashed but no promotion occurred
- Partitions remain without leader

**Diagnosis:**
```bash
# Check if auto_promote is enabled
grep auto_promote /etc/mynadb/config.toml

# Check Meta leader status
curl http://meta-1:9000/api/v1/raft/status

# Check event log
curl "http://meta-1:9000/api/v1/events?topic=nodes&limit=10"
```

**Solutions:**
1. Ensure `auto_promote = true` in config
2. Verify Meta cluster has a leader
3. Check Standby node health (must be Active state)
4. Manually promote: `curl -X POST http://meta-1:9000/api/v1/nodes/{standby}/promote`

### Problem: Wrong Node Assigned as Standby

**Symptoms:**
- High-capacity node assigned as Standby instead of low-capacity one
- Unbalanced resource utilization

**Diagnosis:**
```bash
# List all nodes with labels
curl http://meta-1:9000/api/v1/nodes | jq '.nodes[] | {id: .node_id, mode: .mode, labels}'
```

**Solutions:**
1. Use labels to influence assignment:
```toml
# On the data node config
[node]
labels = { "capacity" = "low", "prefer_standby" = "true" }
```

2. Manually set node mode after registration

### Problem: Frequent Failovers

**Symptoms:**
- Standby promoted, then demoted, repeatedly
- High number of DataNodeModeChanged events

**Diagnosis:**
```bash
# Check failover frequency
curl "http://meta-1:9000/api/v1/events?topic=nodes&event_type=DataNodeModeChanged&limit=50" | \
  jq 'group_by(.payload.node_id) | map({node: .[0].payload.node_id, count: length})'

# Check heartbeat timing
grep heartbeat /var/log/mynadb/meta.log | tail -20
```

**Causes & Solutions:**

| Cause | Solution |
|-------|----------|
| Network instability | Increase lease TTL |
| Node overloaded (slow heartbeats) | Scale up node resources |
| Aggressive health checks | Increase heartbeat_interval_ms |
| GC pauses on JVM tools | Tune GC or use non-JVM monitoring |

### Problem: Partition Data Loss After Failover

**Symptoms:**
- Queries return stale data after failover
- Recent writes missing

**Diagnosis:**
```bash
# Check replication state before failover
curl http://meta-1:9000/api/v1/replicas | \
  jq '.replicas[] | select(.state != "in_sync") | {partition: .partition_key, lag_ms: .replication_lag_ms}'
```

**Solutions:**
1. This is expected for in-flight writes during failover
2. Enable synchronous replication for zero data loss:
```toml
[replication]
mode = "sync"  # Wait for Standby ACK before client ACK
```
3. Use application-level retries for writes during failover

### Problem: Standby Has Stale Data

**Symptoms:**
- After promotion, queries return very old data
- High replication lag before failover

**Diagnosis:**
```bash
# Check replication lag
curl http://meta-1:9000/api/v1/replicas | \
  jq '.replicas[] | {partition: .partition_key, lag_ms: .replication_lag_ms}'
```

**Solutions:**
1. Increase replication thread pool:
```toml
[replication]
max_concurrent_streams = 8
```
2. Check network bandwidth between nodes
3. Monitor disk I/O on Standby nodes

---

## Best Practices

### 1. Capacity Planning

```
CAPACITY PLANNING FORMULA:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Variables:                                                             │
│    W = Write throughput (writes/sec)                                   │
│    Q = Query throughput (queries/sec)                                  │
│    C = Capacity per node                                               │
│    F = Desired fault tolerance (simultaneous failures)                 │
│    R = Primary:Standby ratio                                           │
│                                                                         │
│  Formulas:                                                              │
│    Primary nodes needed = ceil((W + Q) / C)                            │
│    Standby nodes needed = F                                            │
│    Total nodes = Primary + Standby                                     │
│    Recommended ratio = Primary / Standby                               │
│                                                                         │
│  Example:                                                               │
│    W = 100,000 writes/sec                                              │
│    Q = 50,000 queries/sec                                              │
│    C = 50,000 ops/sec per node                                         │
│    F = 2 (tolerate 2 simultaneous failures)                            │
│                                                                         │
│    Primary = ceil(150,000 / 50,000) = 3                                │
│    Standby = 2                                                          │
│    Total = 5                                                            │
│    Ratio = 3:2 (or ~2:1)                                               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2. Ratio Selection Guide

| Scenario | Recommended Ratio | Standbys for 12 nodes |
|----------|-------------------|----------------------|
| Development | 3:1 | 3 Standby, 9 Primary |
| Standard Production | 3:1 | 3 Standby, 9 Primary |
| High Availability | 2:1 | 4 Standby, 8 Primary |
| Critical Systems | 1:1 | 6 Standby, 6 Primary |

### 3. Network Configuration

```
NETWORK BEST PRACTICES:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  1. Low-latency network between Primary and Standby                    │
│     - Same datacenter: < 1ms RTT                                        │
│     - Cross-zone: < 5ms RTT                                            │
│                                                                         │
│  2. Dedicated replication network (if possible)                        │
│     - Separate NIC for replication traffic                             │
│     - QoS priority for replication packets                             │
│                                                                         │
│  3. Health check intervals                                              │
│     - heartbeat_interval_ms > 2 * network_RTT_99th                     │
│     - lease_ttl_ms > 3 * heartbeat_interval_ms                         │
│                                                                         │
│  Example for cross-zone (5ms RTT):                                     │
│     heartbeat_interval_ms = 3000  (3 seconds)                          │
│     lease_ttl_ms = 10000  (10 seconds)                                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4. Zone-Aware Deployment

```yaml
# Kubernetes: Spread Standbys across zones
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mynadb-data
spec:
  template:
    spec:
      topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: DoNotSchedule
        labelSelector:
          matchLabels:
            app: mynadb-data
```

### 5. Monitoring Checklist

```
DAILY MONITORING CHECKLIST:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  [ ] Standby count matches target ratio                                │
│  [ ] All nodes in Active state                                          │
│  [ ] Replication lag < 1 second for all partitions                     │
│  [ ] No failover events in last 24 hours                               │
│  [ ] Disk space > 20% free on all nodes                                │
│  [ ] CPU utilization < 70% on Primary nodes                            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

WEEKLY TASKS:
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  [ ] Test manual failover in staging                                   │
│  [ ] Review failover event history                                      │
│  [ ] Verify backup of Meta cluster state                               │
│  [ ] Check for version updates                                          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 6. Alerting Rules

```yaml
# alerts.yml
groups:
- name: cold_standby
  rules:
  - alert: NoStandbyAvailable
    expr: mynadb_data_nodes_by_mode{mode="standby"} == 0
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "No Standby nodes available"
      description: "All Standby nodes have been promoted or are unavailable"

  - alert: StandbyRatioLow
    expr: |
      mynadb_data_nodes_by_mode{mode="primary"} /
      mynadb_data_nodes_by_mode{mode="standby"} > 4
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Standby ratio below target"
      description: "Current ratio is {{ $value }}:1, expected 3:1 or better"

  - alert: RecentFailover
    expr: increase(mynadb_standby_promotions_total[10m]) > 0
    labels:
      severity: info
    annotations:
      summary: "Standby promoted in last 10 minutes"
      description: "A failover event occurred, verify system health"

  - alert: HighReplicationLag
    expr: mynadb_replication_lag_ms > 5000
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High replication lag on partition {{ $labels.partition }}"
      description: "Lag is {{ $value }}ms, potential data loss on failover"
```

---

## API Reference

### Node Mode Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/nodes` | GET | List all nodes with modes |
| `/api/v1/nodes/{id}` | GET | Get node details |
| `/api/v1/nodes/{id}/mode` | GET | Get node's current mode |
| `/api/v1/nodes/{id}/promote` | POST | Promote Standby to Primary |

### Example Requests

```bash
# List all nodes
curl http://meta-1:9000/api/v1/nodes

# Get specific node mode
curl http://meta-1:9000/api/v1/nodes/data-3/mode

# Promote a Standby
curl -X POST http://meta-1:9000/api/v1/nodes/data-3/promote \
  -H "Content-Type: application/json" \
  -d '{"failed_primary_id": "data-1"}'
```

---

## See Also

- [Architecture: Cold Standby](architecture-cold-standby.md) - Detailed architecture documentation
- [Architecture: Cluster Mode](architecture-cluster.md) - Overall cluster architecture
- [Operations: Backup & Recovery](guide-backup-recovery.md) - Backup procedures
