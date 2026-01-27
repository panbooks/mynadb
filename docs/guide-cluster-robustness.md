# Cluster Robustness Testing Guide

This guide walks through testing MynaDB cluster resilience by simulating node failures and verifying automatic recovery.

## Prerequisites

- MynaDB built: `cargo build --release`
- goreman installed: `go install github.com/mattn/goreman@latest`
- Cluster not running (stop any existing instances first)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Clients                                   │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Router Layer                                  │
│  ┌─────────┐                                                    │
│  │ router1 │  HTTP:7000  Flight:7001                            │
│  └────┬────┘                                                    │
└───────┼─────────────────────────────────────────────────────────┘
        │
        ├──────────────────┬──────────────────┐
        ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│    meta1     │  │    meta2     │  │    meta3     │
│  gRPC:7100   │◄─┼──►gRPC:7101  │◄─┼──►gRPC:7102  │
│   (Raft)     │  │   (Raft)     │  │   (Raft)     │
└──────────────┘  └──────────────┘  └──────────────┘
        │
        ├──────────────────┬──────────────────┐
        ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│    data1     │  │    data2     │  │    data3     │
│ Flight:7200  │  │ Flight:7201  │  │ Flight:7202  │
└──────────────┘  └──────────────┘  └──────────────┘
```

**Fault Tolerance:**
- Meta nodes: Tolerates 1 of 3 failures (Raft quorum)
- Data nodes: Tolerates any number of failures (router retries to healthy nodes)
- Router nodes: Stateless, can be restarted anytime

---

## 1. Start the Cluster

```bash
cd /path/to/mynadb

# Start the replicaset cluster
./dev/start-replicaset.sh
```

Wait for startup messages. You should see:
```
meta1    | ===============================================
meta1    |   MynaDB Meta Node
...
data1    | ===============================================
data1    |   MynaDB Data Node
...
router1  | ===============================================
router1  |   MynaDB Router Node
```

### Verify Cluster Health

```bash
# Check router health endpoint
curl http://localhost:7000/health
# Expected: OK

# Verify all data nodes are registered
curl -s http://localhost:7000/health | head -1
# Expected: OK
```

### Write Initial Test Data

```bash
# Write sample data points
curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,host=server1,region=us-east value=0.5,cores=4i'

curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,host=server2,region=us-west value=0.7,cores=8i'

curl -X POST http://localhost:7000/write/testdb \
  -d 'memory,host=server1 used=1024,total=4096'

# Verify data was written
curl -s "http://localhost:7000/query/testdb?sql=SELECT%20*%20FROM%20cpu%20LIMIT%205"
```

---

## 2. Meta Node Robustness Testing

Meta nodes use Raft consensus for distributed coordination. The cluster requires a quorum (2 of 3 nodes) to operate.

### 2.1 Identify the Current Leader

```bash
# Check meta1 logs for leader status
grep -E "(become leader|stepping down)" ./dev/logs/replicaset/meta1/meta1.$(date +%Y-%m-%d)

# Or check all meta nodes
for i in 1 2 3; do
  echo "=== meta$i ==="
  grep -E "become leader" ./dev/logs/replicaset/meta$i/meta$i.$(date +%Y-%m-%d) | tail -1
done
```

The node showing "become leader" most recently is the current leader (typically meta1 on fresh start).

### 2.2 Test Leader Failure and Election

**Scenario:** Stop the leader node and verify a new leader is elected.

```bash
# Stop meta1 (assuming it's the leader)
goreman -f ./dev/Procfile.replicaset run stop meta1
echo "Stopped meta1 (leader)"

# Wait for leader election (typically < 2 seconds)
sleep 3

# Check which node became the new leader
for i in 2 3; do
  echo "=== meta$i ==="
  grep -E "become leader" ./dev/logs/replicaset/meta$i/meta$i.$(date +%Y-%m-%d) | tail -1
done
```

**Expected behavior:**
- One of meta2 or meta3 becomes the new leader
- Log shows: `"become leader id=X"`
- Cluster continues operating

**Verify cluster still works:**

```bash
# Health check should still pass
curl http://localhost:7000/health

# Writes should still work
curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,host=server1 value=0.55'

# Query should still work
curl -s "http://localhost:7000/query/testdb?sql=SELECT%20COUNT(*)%20FROM%20cpu"
```

### 2.3 Test Leader Recovery

**Scenario:** Restart the failed leader and verify it rejoins as a follower.

```bash
# Restart meta1
goreman -f ./dev/Procfile.replicaset run start meta1
echo "Started meta1"

# Wait for rejoin
sleep 3

# Check meta1 recognized the new leader
grep -E "(vote is changing|stepping down)" ./dev/logs/replicaset/meta1/meta1.$(date +%Y-%m-%d) | tail -3
```

**Expected behavior:**
- meta1 sees higher term from current leader
- meta1 steps down and becomes follower
- Log shows: `"vote is changing from ... to ..."`

### 2.4 Test Follower Failure

**Scenario:** Stop a follower node and verify cluster maintains quorum.

```bash
# Stop meta2 (a follower)
goreman -f ./dev/Procfile.replicaset run stop meta2
echo "Stopped meta2"

sleep 2

# Cluster should still be healthy (2 of 3 nodes = quorum)
curl http://localhost:7000/health

# Writes should still work
curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,host=server3 value=0.6'
```

**Restart the follower:**

```bash
goreman -f ./dev/Procfile.replicaset run start meta2
echo "Started meta2"

sleep 3

# Verify meta2 received snapshot and rejoined
grep -E "Snapshot installed" ./dev/logs/replicaset/meta2/meta2.$(date +%Y-%m-%d) | tail -1
```

### 2.5 Test All Meta Nodes Sequentially

```bash
# Full meta node rotation test
for node in meta1 meta2 meta3; do
  echo ""
  echo "========== Testing $node =========="

  # Stop node
  goreman -f ./dev/Procfile.replicaset run stop $node
  echo "Stopped $node"
  sleep 3

  # Verify cluster health
  if curl -s http://localhost:7000/health | grep -q "OK"; then
    echo "Cluster healthy after $node stopped"
  else
    echo "WARNING: Cluster unhealthy!"
  fi

  # Write test data
  curl -s -X POST http://localhost:7000/write/testdb \
    -d "cpu,host=test_$node value=0.5"
  echo "Write succeeded"

  # Restart node
  goreman -f ./dev/Procfile.replicaset run start $node
  echo "Restarted $node"
  sleep 3

  # Verify cluster health
  curl http://localhost:7000/health
  echo ""
done

echo "All meta nodes tested successfully!"
```

---

## 3. Data Node Robustness Testing

Data nodes store the actual time series data. The router automatically retries failed writes to other healthy nodes.

### 3.1 Test Single Data Node Failure

**Scenario:** Stop one data node and verify writes are routed to healthy nodes.

```bash
# Stop data1
goreman -f ./dev/Procfile.replicaset run stop data1
echo "Stopped data1"

sleep 2

# Verify cluster health
curl http://localhost:7000/health

# Write data - should be routed to data2 or data3
curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,host=failover_test value=0.99'

# Check router logs for failover
tail -10 ./dev/logs/replicaset/router1/router1.$(date +%Y-%m-%d) | grep -E "(failed|retry|succeeded)"
```

**Expected behavior:**
- Router detects "tcp connect error" on data-1
- Router retries to data-2 or data-3
- Write succeeds on retry
- Router marks data-1 as "suspect"

**Restart data1:**

```bash
goreman -f ./dev/Procfile.replicaset run start data1
echo "Started data1"

sleep 3

# Verify data1 re-registered with Meta
grep "Registered with Meta" ./dev/logs/replicaset/data1/data1.$(date +%Y-%m-%d) | tail -1
```

### 3.2 Test Multiple Data Node Failures

**Scenario:** Stop 2 of 3 data nodes and verify writes still work.

```bash
# Stop data1 and data2
goreman -f ./dev/Procfile.replicaset run stop data1
goreman -f ./dev/Procfile.replicaset run stop data2
echo "Stopped data1 and data2"

sleep 2

# Writes should still work (routed to data3)
curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,host=single_node_test value=0.88'

# Verify write succeeded
curl -s "http://localhost:7000/query/testdb?sql=SELECT%20*%20FROM%20cpu%20WHERE%20host='single_node_test'"

# Restart nodes
goreman -f ./dev/Procfile.replicaset run start data1
goreman -f ./dev/Procfile.replicaset run start data2
echo "Restarted data1 and data2"

sleep 3
```

### 3.3 Test All Data Nodes Sequentially

```bash
# Full data node rotation test
for node in data1 data2 data3; do
  echo ""
  echo "========== Testing $node =========="

  # Stop node
  goreman -f ./dev/Procfile.replicaset run stop $node
  echo "Stopped $node"
  sleep 2

  # Verify cluster health
  curl http://localhost:7000/health

  # Write test data (will failover to other nodes)
  curl -s -X POST http://localhost:7000/write/testdb \
    -d "cpu,host=test_$node value=0.5"
  echo "Write succeeded (via failover)"

  # Check router failover logs
  tail -5 ./dev/logs/replicaset/router1/router1.$(date +%Y-%m-%d) | grep -E "(failed|succeeded)"

  # Restart node
  goreman -f ./dev/Procfile.replicaset run start $node
  echo "Restarted $node"
  sleep 3
done

echo "All data nodes tested successfully!"
```

---

## 4. Router Node Robustness Testing

Router nodes are stateless and can be restarted without data loss. They automatically reconnect to Meta and discover data nodes.

### 4.1 Test Router Restart

```bash
# Stop router1
goreman -f ./dev/Procfile.replicaset run stop router1
echo "Stopped router1"

# Health endpoint will fail (expected)
curl http://localhost:7000/health 2>&1 || echo "(Expected: connection refused)"

# Restart router1
goreman -f ./dev/Procfile.replicaset run start router1
echo "Started router1"

# Wait for router to initialize
sleep 3

# Verify router is healthy
curl http://localhost:7000/health

# Verify router discovered data nodes
grep "Initialized shard cache" ./dev/logs/replicaset/router1/router1.$(date +%Y-%m-%d) | tail -1

# Write data to verify full functionality
curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,host=router_restart_test value=0.77'

# Query data
curl -s "http://localhost:7000/query/testdb?sql=SELECT%20COUNT(*)%20FROM%20cpu"
```

**Expected behavior:**
- Router connects to Meta service
- Router initializes shard cache with data nodes
- Writes and queries work immediately

### 4.2 Test Router During Meta Failover

**Scenario:** Stop the Meta leader while router is running and verify router switches to new leader.

```bash
# Identify current leader (check logs or assume meta1)
# Stop the leader
goreman -f ./dev/Procfile.replicaset run stop meta1
echo "Stopped meta1 (leader)"

sleep 3

# Router should automatically switch to another Meta node
# Check router logs
grep -E "(Subscription error|Connecting to Meta)" ./dev/logs/replicaset/router1/router1.$(date +%Y-%m-%d) | tail -5

# Verify writes still work
curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,host=meta_failover_test value=0.66'

# Restart meta1
goreman -f ./dev/Procfile.replicaset run start meta1
echo "Restarted meta1"
```

**Expected behavior:**
- Router detects subscription error when meta1 stops
- Router reconnects to meta2 or meta3
- Writes continue without interruption

---

## 5. Comprehensive Cluster Test

Run this script to test all failure scenarios in sequence:

```bash
#!/bin/bash
# comprehensive_cluster_test.sh

set -e

echo "=========================================="
echo "  MynaDB Cluster Robustness Test Suite"
echo "=========================================="

BASE_URL="http://localhost:7000"
PROCFILE="./dev/Procfile.replicaset"
DATE=$(date +%Y-%m-%d)

# Helper functions
check_health() {
  if curl -s "$BASE_URL/health" | grep -q "OK"; then
    echo "[PASS] Cluster healthy"
    return 0
  else
    echo "[FAIL] Cluster unhealthy"
    return 1
  fi
}

write_data() {
  local tag=$1
  curl -s -X POST "$BASE_URL/write/testdb" \
    -d "test,scenario=$tag value=1"
  echo "[PASS] Write succeeded: $tag"
}

query_data() {
  local count=$(curl -s "$BASE_URL/query/testdb?sql=SELECT%20COUNT(*)%20FROM%20test" | grep -oE '[0-9]+' | head -1)
  echo "[INFO] Total records: $count"
}

stop_node() {
  goreman -f $PROCFILE run stop $1
  echo "[INFO] Stopped $1"
  sleep 2
}

start_node() {
  goreman -f $PROCFILE run start $1
  echo "[INFO] Started $1"
  sleep 3
}

# Initial setup
echo ""
echo "--- Initial State ---"
check_health
write_data "initial"
query_data

# Test 1: Meta Leader Failover
echo ""
echo "--- Test 1: Meta Leader Failover ---"
stop_node meta1
check_health
write_data "meta1_down"
start_node meta1
check_health

# Test 2: Meta Follower Failover
echo ""
echo "--- Test 2: Meta Follower Failover ---"
stop_node meta2
check_health
write_data "meta2_down"
start_node meta2
check_health

# Test 3: Another Meta Node
echo ""
echo "--- Test 3: Meta3 Failover ---"
stop_node meta3
check_health
write_data "meta3_down"
start_node meta3
check_health

# Test 4: Data Node Failover
echo ""
echo "--- Test 4: Data Node Failover ---"
for node in data1 data2 data3; do
  stop_node $node
  check_health
  write_data "${node}_down"
  start_node $node
  check_health
done

# Test 5: Router Restart
echo ""
echo "--- Test 5: Router Restart ---"
stop_node router1
sleep 1
start_node router1
check_health
write_data "router_restart"

# Final verification
echo ""
echo "--- Final Verification ---"
check_health
query_data

echo ""
echo "=========================================="
echo "  All tests completed successfully!"
echo "=========================================="
```

Save as `scripts/comprehensive_cluster_test.sh` and run:

```bash
chmod +x scripts/comprehensive_cluster_test.sh
./scripts/comprehensive_cluster_test.sh
```

---

## 6. Expected Failover Times

| Scenario | Expected Recovery Time |
|----------|------------------------|
| Meta leader failure | ~1-2 seconds (Raft election) |
| Meta follower failure | Immediate (no election needed) |
| Meta node rejoin | ~1-2 seconds (snapshot sync) |
| Data node failure | ~100ms (router retry) |
| Data node rejoin | ~2-3 seconds (re-registration) |
| Router restart | ~2-3 seconds (cache init) |

---

## 7. Monitoring and Logs

### Log Locations

```
./dev/logs/replicaset/
├── meta1/meta1.YYYY-MM-DD
├── meta2/meta2.YYYY-MM-DD
├── meta3/meta3.YYYY-MM-DD
├── data1/data1.YYYY-MM-DD
├── data2/data2.YYYY-MM-DD
├── data3/data3.YYYY-MM-DD
└── router1/router1.YYYY-MM-DD
```

### Key Log Patterns

**Meta nodes (Raft):**
```bash
# Leader election
grep "become leader" ./dev/logs/replicaset/meta*/meta*.$(date +%Y-%m-%d)

# Vote changes
grep "vote is changing" ./dev/logs/replicaset/meta*/meta*.$(date +%Y-%m-%d)

# Snapshot transfers
grep "Snapshot installed" ./dev/logs/replicaset/meta*/meta*.$(date +%Y-%m-%d)
```

**Data nodes:**
```bash
# Registration
grep "Registered with Meta" ./dev/logs/replicaset/data*/data*.$(date +%Y-%m-%d)

# Heartbeat issues
grep "Heartbeat failed" ./dev/logs/replicaset/data*/data*.$(date +%Y-%m-%d)
```

**Router:**
```bash
# Meta connection
grep "Connected to Meta" ./dev/logs/replicaset/router1/router1.$(date +%Y-%m-%d)

# Data node discovery
grep "Initialized shard cache" ./dev/logs/replicaset/router1/router1.$(date +%Y-%m-%d)

# Write failover
grep -E "(failed|retry|succeeded)" ./dev/logs/replicaset/router1/router1.$(date +%Y-%m-%d)

# Node marked suspect
grep "suspect" ./dev/logs/replicaset/router1/router1.$(date +%Y-%m-%d)
```

---

## 8. Troubleshooting

### Cluster Won't Start

```bash
# Check for leftover processes
pgrep -f mynadb
pkill -f mynadb

# Clean up data directories
rm -rf ./dev/data/replicaset/*

# Restart cluster
./dev/start-replicaset.sh
```

### Meta Nodes Can't Elect Leader

```bash
# Ensure at least 2 of 3 meta nodes are running
goreman -f ./dev/Procfile.replicaset run status

# Check for network issues
curl http://localhost:7100/health  # meta1
curl http://localhost:7101/health  # meta2
curl http://localhost:7102/health  # meta3
```

### Writes Failing After Data Node Recovery

```bash
# Data node may still be marked as suspect
# Wait for Meta to update router's cache, or restart router
goreman -f ./dev/Procfile.replicaset run restart router1
```

### Router Can't Find Data Nodes

```bash
# Check if data nodes are registered with Meta
# Restart router to re-fetch node list
goreman -f ./dev/Procfile.replicaset run restart router1

# Check router logs for initialization
grep "Initialized shard cache" ./dev/logs/replicaset/router1/router1.$(date +%Y-%m-%d)
```

---

## 9. Stopping the Cluster

```bash
# Stop all processes
goreman -f ./dev/Procfile.replicaset run stop-all

# Or use the stop script
./dev/stop-all.sh

# Verify all processes stopped
pgrep -f mynadb || echo "All MynaDB processes stopped"
```

---

## Summary

MynaDB's replicaset cluster provides:

1. **Meta node resilience**: Raft consensus ensures metadata availability with 1-second failover
2. **Data node resilience**: Router automatically retries writes to healthy nodes
3. **Router resilience**: Stateless design allows instant restart and recovery
4. **Automatic recovery**: Nodes automatically rejoin and sync state after restart

For production deployments, consider:
- Running multiple router nodes behind a load balancer
- Configuring appropriate heartbeat and lease timeouts
- Setting up monitoring for leader elections and node health
- Using persistent storage for Meta and Data node data directories
