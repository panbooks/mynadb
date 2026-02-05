# Master-Slave HA Local Testing Guide

This guide explains how to test Master-Slave HA mode on a single Linux server.

## Overview

Run both master and slave instances on the same machine using different ports and data directories.

```
┌─────────────────────────────────────────────────────────────┐
│                     Single Linux Host                        │
│                                                              │
│  ┌─────────────────────┐      ┌─────────────────────┐       │
│  │      MASTER         │      │       SLAVE         │       │
│  │  HTTP:  7000        │      │  HTTP:  7100        │       │
│  │  Flight: 7001       │      │  Flight: 7101       │       │
│  │  Repl:  7003        │ ───▶ │  (connects to 7003) │       │
│  │  Data: /tmp/ha-test/master │  Data: /tmp/ha-test/slave  │
│  └─────────────────────┘      └─────────────────────┘       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Run the automated test script
./scripts/test_ha_local.sh
```

## Manual Testing

### Step 1: Create Test Directory

```bash
mkdir -p /tmp/ha-test/{master,slave}
```

### Step 2: Create Master Config

Create `/tmp/ha-test/config-master.toml`:

```toml
[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "/tmp/ha-test/master/data"

[logging]
log_dir = "/tmp/ha-test/master/logs"
level = "debug"

[cache]
flush_interval_secs = 10
max_size_mb = 100

[wal]
enabled = true
sync_on_write = false

[ha]
enabled = true
role = "master"
node_id = "master-01"

[ha.replication]
mode = "async"
listen_addr = "127.0.0.1:7003"

[ha.heartbeat]
interval_secs = 2
timeout_secs = 10
```

### Step 3: Create Slave Config

Create `/tmp/ha-test/config-slave.toml`:

```toml
[server]
http_port = 7100
flight_port = 7101

[storage]
data_dir = "/tmp/ha-test/slave/data"

[logging]
log_dir = "/tmp/ha-test/slave/logs"
level = "debug"

[cache]
flush_interval_secs = 10
max_size_mb = 100

[wal]
enabled = true
sync_on_write = false

[ha]
enabled = true
role = "slave"
node_id = "slave-01"

[ha.replication]
master_addr = "127.0.0.1:7003"
reconnect_interval_secs = 5

[ha.heartbeat]
interval_secs = 2
timeout_secs = 10

[ha.failover]
mode = "manual"
failure_threshold = 3
promotion_delay_secs = 5
```

### Step 4: Start Master

```bash
# Terminal 1
cd /path/to/stats-service
./target/release/tsdb-stats -c /tmp/ha-test/config-master.toml
```

Expected output:
```
✅ HA enabled: role=Master, node_id=master-01
Replication server listening on 127.0.0.1:7003
```

### Step 5: Start Slave

```bash
# Terminal 2
cd /path/to/stats-service
./target/release/tsdb-stats -c /tmp/ha-test/config-slave.toml
```

Expected output:
```
✅ HA enabled: role=Slave, node_id=slave-01
Connecting to master at 127.0.0.1:7003
Handshake accepted by master master-01
HA slave replay task started
```

### Step 6: Verify Connection

```bash
# Check master status
curl -s http://localhost:7000/ha/status | jq .

# Expected:
{
  "enabled": true,
  "node_id": "master-01",
  "role": "master",
  "state": "running",
  "replication": {
    "role": "master",
    "current_sequence": 0,
    "slave_count": 1,
    "connected": true
  }
}

# Check slave status
curl -s http://localhost:7100/ha/status | jq .

# Expected:
{
  "enabled": true,
  "node_id": "slave-01",
  "role": "slave",
  "state": "running",
  "replication": {
    "role": "slave",
    "current_sequence": 0,
    "connected": true,
    "seconds_since_heartbeat": 1
  }
}
```

### Step 7: Test Replication

```bash
# Write data to master
curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,host=server1,region=us-east value=42.5
cpu,host=server2,region=us-west value=38.2
memory,host=server1 used=8589934592,total=17179869184'

# Wait for replication (1-2 seconds)
sleep 2

# Query from slave
curl -s "http://localhost:7100/query/testdb?sql=SELECT * FROM default.cpu" | jq .

# Query from master (should be same)
curl -s "http://localhost:7000/query/testdb?sql=SELECT * FROM default.cpu" | jq .
```

### Step 8: Test Write Rejection on Slave

```bash
# Try to write to slave (should fail with 503)
curl -v -X POST http://localhost:7100/write/testdb \
  -d 'cpu,host=server3 value=50'

# Expected: 503 Service Unavailable
# "This node is a slave; writes must be sent to the master"
```

### Step 9: Test Failover

```bash
# Stop the master (Ctrl+C in Terminal 1)

# Check slave status (will show disconnected)
curl -s http://localhost:7100/ha/status | jq .

# Promote slave to master
curl -X POST http://localhost:7100/ha/promote \
  -H "Content-Type: application/json" \
  -d '{"confirm": true}'

# Expected: {"status": "success", "message": "Promoted to master successfully"}

# Verify new master status
curl -s http://localhost:7100/ha/status | jq .
# role should now be "master"

# Write to new master (should work)
curl -X POST http://localhost:7100/write/testdb \
  -d 'cpu,host=server4,region=eu-west value=55.0'

# Query to verify
curl -s "http://localhost:7100/query/testdb?sql=SELECT * FROM default.cpu ORDER BY time" | jq .
```

## Test Scenarios

### Scenario 1: Basic Replication
1. Start master and slave
2. Write 1000 points to master
3. Verify all points appear on slave
4. Compare row counts

### Scenario 2: Reconnection
1. Start master and slave
2. Stop slave
3. Write data to master
4. Restart slave
5. Verify slave catches up

### Scenario 3: Manual Failover
1. Start master and slave
2. Write data to master
3. Stop master
4. Promote slave
5. Write data to new master
6. Verify data integrity

### Scenario 4: Automatic Failover
1. Configure slave with `failover.mode = "automatic"`
2. Start master and slave
3. Stop master abruptly
4. Wait for automatic promotion (~30 seconds with default settings)
5. Verify slave became master

## Troubleshooting

### Slave Cannot Connect

```
Error: Failed to connect to master: Connection refused
```

**Solutions:**
1. Verify master is running: `curl http://localhost:7000/health`
2. Check master is listening on replication port: `netstat -tlnp | grep 7003`
3. Verify `master_addr` in slave config matches master's `listen_addr`

### Replication Lag

Check sequence numbers:
```bash
MASTER_SEQ=$(curl -s http://localhost:7000/ha/status | jq .replication.current_sequence)
SLAVE_SEQ=$(curl -s http://localhost:7100/ha/status | jq .replication.current_sequence)
echo "Lag: $((MASTER_SEQ - SLAVE_SEQ)) entries"
```

### Promotion Fails

```
{"status": "not_allowed", "reason": "..."}
```

**Solutions:**
1. Verify slave is in correct state (running or syncing)
2. Check failover mode is not "disabled"
3. Check logs for detailed error

## Cleanup

```bash
# Stop both processes (Ctrl+C)

# Remove test data
rm -rf /tmp/ha-test
```

## Performance Testing

### Write Throughput

```bash
# Generate load to master
for i in {1..1000}; do
  curl -s -X POST http://localhost:7000/write/loadtest \
    -d "metric,host=h$((i%10)) value=$RANDOM" > /dev/null
done

# Check replication lag
curl -s http://localhost:7000/ha/status | jq .replication.current_sequence
curl -s http://localhost:7100/ha/status | jq .replication.current_sequence
```

### Measure Failover Time

```bash
# Start timing
START=$(date +%s.%N)

# Kill master
pkill -f "config-master.toml"

# Wait for automatic failover (if configured)
while true; do
  ROLE=$(curl -s http://localhost:7100/ha/status 2>/dev/null | jq -r .role)
  if [ "$ROLE" = "master" ]; then
    break
  fi
  sleep 0.5
done

END=$(date +%s.%N)
echo "Failover time: $(echo "$END - $START" | bc) seconds"
```
