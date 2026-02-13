# Master-Slave High Availability Guide

This guide covers the configuration, deployment, and operation of MynaDB in Master-Slave HA mode.

## Overview

Master-Slave HA provides high availability through WAL (Write-Ahead Log) replication:

```
┌─────────────────┐                    ┌─────────────────┐
│     MASTER      │                    │      SLAVE      │
│   (Active)      │ ──── WAL Stream ──▶│   (Standby)     │
│                 │                    │                 │
│  Accepts writes │                    │  Read replicas  │
│  Port 7000      │                    │  Port 7000      │
│  Repl: 7003     │                    │                 │
└─────────────────┘                    └─────────────────┘
```

**Key Features:**
- Real-time WAL replication over TCP
- Automatic or manual failover
- Heartbeat-based failure detection
- Checksum verification for data integrity
- HTTP API for status and promotion

## Configuration

### Master Node Configuration

```toml
# config-master.toml

[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "/data/stats"

[ha]
enabled = true
role = "master"
node_id = "master-01"

[ha.replication]
mode = "async"                    # "async" or "sync"
listen_addr = "0.0.0.0:7003"      # Replication server address
timeout_ms = 5000                 # Sync mode ACK timeout

[ha.heartbeat]
interval_secs = 2                 # Heartbeat frequency
timeout_secs = 10                 # Heartbeat timeout threshold

[ha.failover]
mode = "manual"                   # Master doesn't need failover
```

### Slave Node Configuration

```toml
# config-slave.toml

[server]
http_port = 7000
flight_port = 7001

[storage]
data_dir = "/data/stats"

[ha]
enabled = true
role = "slave"
node_id = "slave-01"

[ha.replication]
master_addr = "master-host:7003"  # Master's replication address
reconnect_interval_secs = 5       # Reconnect delay on connection loss

[ha.heartbeat]
interval_secs = 2
timeout_secs = 10

[ha.failover]
mode = "automatic"                # "automatic", "manual", or "disabled"
failure_threshold = 3             # Missed heartbeats before failover
promotion_delay_secs = 5          # Delay before promotion starts
```

## Configuration Reference

### HaConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable HA mode |
| `role` | string | "master" | Node role: "master" or "slave" |
| `node_id` | string | hostname | Unique node identifier |

### ReplicationConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | string | "async" | Replication mode: "sync" or "async" |
| `listen_addr` | string | "0.0.0.0:7003" | Master: TCP listen address |
| `master_addr` | string | null | Slave: Master's replication address |
| `timeout_ms` | u64 | 5000 | Sync mode ACK timeout (ms) |
| `reconnect_interval_secs` | u64 | 5 | Slave reconnect delay (seconds) |

### HeartbeatConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `interval_secs` | u64 | 2 | Heartbeat send interval |
| `timeout_secs` | u64 | 10 | Heartbeat timeout threshold |

### FailoverConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | string | "automatic" | Failover mode: "automatic", "manual", "disabled" |
| `failure_threshold` | u32 | 3 | Missed heartbeats before failover |
| `promotion_delay_secs` | u64 | 5 | Delay before promotion |

## Deployment

### Starting Master Node

```bash
# Start master
./tsdb-stats -c config-master.toml

# Expected output:
# ✅ HA enabled: role=Master, node_id=master-01
# Replication server listening on 0.0.0.0:7003
```

### Starting Slave Node

```bash
# Start slave (after master is running)
./tsdb-stats -c config-slave.toml

# Expected output:
# ✅ HA enabled: role=Slave, node_id=slave-01
# Connecting to master at master-host:7003
# Handshake accepted by master master-01
```

### Verifying Replication

```bash
# Check master status
curl http://master:7000/ha/status

# Response:
{
  "enabled": true,
  "node_id": "master-01",
  "role": "master",
  "state": "running",
  "replication": {
    "role": "master",
    "current_sequence": 12345,
    "slave_count": 1,
    "connected": true
  }
}

# Check slave status
curl http://slave:7000/ha/status

# Response:
{
  "enabled": true,
  "node_id": "slave-01",
  "role": "slave",
  "state": "running",
  "replication": {
    "role": "slave",
    "current_sequence": 12345,
    "connected": true,
    "seconds_since_heartbeat": 1
  }
}
```

## API Endpoints

### GET /ha/status

Returns detailed HA status information.

```bash
curl http://localhost:7000/ha/status
```

### GET /ha/health

Health check for load balancers. Returns 200 if healthy, 503 if unhealthy.

```bash
curl http://localhost:7000/ha/health

# Master response:
{"status": "healthy", "role": "master", "ready": true}

# Slave connected response:
{"status": "healthy", "role": "slave", "connected": true, "ready": true}

# Slave disconnected response (503):
{"status": "unhealthy", "role": "slave", "connected": false, "ready": false}
```

### GET /ha/is-master

Check if node is master (for routing writes).

```bash
curl http://localhost:7000/ha/is-master

# Master response:
{"is_master": true, "ready_for_writes": true}

# Slave response:
{"is_master": false, "ready_for_writes": false}
```

### POST /ha/promote

Manually promote slave to master.

```bash
curl -X POST http://slave:7000/ha/promote \
  -H "Content-Type: application/json" \
  -d '{"confirm": true}'

# Success response:
{"status": "success", "message": "Promoted to master successfully"}
```

## Failover

### Automatic Failover

When `failover.mode = "automatic"`:

1. Slave monitors master heartbeats
2. After `failure_threshold` missed heartbeats (default: 3)
3. Slave automatically promotes itself to master
4. Starts accepting writes on port 7000

**Timeline with defaults:**
- Heartbeat interval: 2 seconds
- Timeout: 10 seconds
- Threshold: 3 missed
- **Total failover time: ~30-40 seconds**

### Manual Failover

When `failover.mode = "manual"`:

1. Verify master is truly unavailable
2. Promote slave manually:

```bash
# Promote slave to master
curl -X POST http://slave:7000/ha/promote \
  -H "Content-Type: application/json" \
  -d '{"confirm": true}'
```

3. Update client connections to new master

### Failback (Recovery)

After original master recovers:

**Option 1: Keep new master, demote old master**
1. Reconfigure old master as slave
2. Update `role = "slave"` and `master_addr`
3. Restart old master

**Option 2: Restore original master**
1. Stop new master (slave)
2. Sync data if needed
3. Restart original master
4. Reconfigure and restart slave

## Load Balancer Integration

### HAProxy Configuration

```haproxy
frontend stats_write
    bind *:7000
    default_backend stats_master

frontend stats_read
    bind *:7002
    default_backend stats_all

backend stats_master
    option httpchk GET /ha/is-master
    http-check expect json-query $.is_master eq true
    server master master:7000 check
    server slave slave:7000 check backup

backend stats_all
    balance roundrobin
    option httpchk GET /ha/health
    server master master:7000 check
    server slave slave:7000 check
```

### Nginx Configuration

```nginx
upstream stats_write {
    server master:7000;
    server slave:7000 backup;
}

upstream stats_read {
    server master:7000;
    server slave:7000;
}

server {
    listen 7000;

    location /write {
        proxy_pass http://stats_write;
    }

    location /query {
        proxy_pass http://stats_read;
    }
}
```

## Monitoring

### Key Metrics to Monitor

1. **Replication Lag**
   - Compare `current_sequence` between master and slave
   - Alert if lag exceeds threshold

2. **Connection Status**
   - Monitor `/ha/health` endpoint
   - Alert on `connected: false`

3. **Heartbeat Latency**
   - Monitor `seconds_since_heartbeat`
   - Alert if > `timeout_secs / 2`

### Prometheus Metrics (Future)

```
# Replication sequence
stats_ha_replication_sequence{node="master-01"} 12345

# Slave count
stats_ha_slave_count{node="master-01"} 1

# Connection status
stats_ha_connected{node="slave-01"} 1

# Heartbeat latency
stats_ha_heartbeat_latency_seconds{node="slave-01"} 1.2
```

### Health Check Script

```bash
#!/bin/bash
# check_ha_health.sh

MASTER_HOST=${1:-localhost}
SLAVE_HOST=${2:-localhost}

echo "=== Master Status ==="
curl -s http://$MASTER_HOST:7000/ha/status | jq .

echo ""
echo "=== Slave Status ==="
curl -s http://$SLAVE_HOST:7000/ha/status | jq .

echo ""
echo "=== Replication Lag ==="
MASTER_SEQ=$(curl -s http://$MASTER_HOST:7000/ha/status | jq .replication.current_sequence)
SLAVE_SEQ=$(curl -s http://$SLAVE_HOST:7000/ha/status | jq .replication.current_sequence)
echo "Master sequence: $MASTER_SEQ"
echo "Slave sequence:  $SLAVE_SEQ"
echo "Lag: $((MASTER_SEQ - SLAVE_SEQ)) entries"
```

## Troubleshooting

### Slave Cannot Connect to Master

```
Error: Failed to connect to master: Connection refused
```

**Solutions:**
1. Verify master is running and listening on replication port
2. Check firewall rules allow port 7003
3. Verify `master_addr` in slave config is correct
4. Check network connectivity: `telnet master-host 7003`

### Handshake Rejected

```
Error: Handshake rejected: Unknown error
```

**Solutions:**
1. Check protocol version compatibility
2. Verify both nodes are running same MynaDB version
3. Check master logs for rejection reason

### High Replication Lag

**Symptoms:**
- Slave `current_sequence` significantly behind master
- Queries on slave return stale data

**Solutions:**
1. Check network bandwidth between nodes
2. Increase slave's processing capacity
3. Consider async replication if using sync mode
4. Check for slow disk I/O on slave

### Split-Brain Prevention

In automatic failover, split-brain can occur if:
- Network partition isolates master
- Slave promotes while master still running

**Prevention:**
1. Use `failover.mode = "manual"` for critical deployments
2. Implement fencing (STONITH) for old master
3. Use quorum-based systems (etcd/Consul) for leader election
4. Set higher `failure_threshold` to reduce false positives

## Best Practices

1. **Use Unique Node IDs**
   - Set meaningful `node_id` for each node
   - Helps with log correlation and debugging

2. **Configure Appropriate Timeouts**
   - Higher thresholds reduce false failovers
   - Lower thresholds reduce downtime
   - Balance based on your requirements

3. **Monitor Replication Lag**
   - Set up alerts for lag exceeding thresholds
   - Investigate persistent lag

4. **Test Failover Regularly**
   - Practice manual failover procedures
   - Verify automatic failover works as expected

5. **Secure Replication Traffic**
   - Use VPN or private network for replication
   - Consider TLS for replication (future feature)

6. **Backup Strategy**
   - Continue regular backups despite HA
   - HA protects against node failure, not data corruption

## Limitations

1. **Single Slave Only** (current implementation)
   - Future: Support multiple slaves

2. **No Automatic Failback**
   - Manual intervention required to restore original master

3. **No TLS for Replication**
   - Use network-level encryption (VPN, private network)

4. **No Quorum/Consensus**
   - Split-brain possible in network partitions
   - Consider external coordination (etcd/Consul) for production

## Architecture Details

### Replication Protocol

Binary protocol using bincode serialization:

```
┌──────────────┬─────────────────────────────────────┐
│ Length (4B)  │ WireMessage (bincode serialized)    │
└──────────────┴─────────────────────────────────────┘
```

**Message Types:**
- `Handshake` / `HandshakeResponse` - Connection setup
- `WalEntry` - Replicated write data
- `Ack` - Acknowledgment from slave
- `Heartbeat` / `HeartbeatResponse` - Liveness check

### WAL Entry Structure

```rust
struct ReplicatedWalEntry {
    sequence: u64,          // Global ordering
    timestamp_nanos: u64,   // Entry timestamp
    entry_type: u8,         // 0=DataWrite, 1=SeriesTrack
    db_name: String,        // Target database
    payload: Vec<u8>,       // Line protocol data
    checksum: u32,          // CRC32 verification
}
```

### State Machine

```
                    ┌──────────────┐
                    │ Initializing │
                    └──────┬───────┘
                           │
                           ▼
┌──────────┐         ┌─────────────┐         ┌──────────┐
│ Demoting │◀────────│   Running   │────────▶│ Promoting│
└──────────┘         └─────────────┘         └──────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   Syncing    │ (slave only)
                    └──────────────┘
```
