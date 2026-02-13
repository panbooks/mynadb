# Master-Slave Failover Demo Guide

This guide provides detailed demos for different failover approaches in the Master-Slave HA architecture.

---

## Failover Options Overview

There are **3 approaches** with different trade-offs:

| Approach | Coordinator | Load Balancer | Manual Work | Complexity |
|----------|-------------|---------------|-------------|------------|
| **Option 1: Fully Automatic** | Required (etcd) | Required (HAProxy) | None | Medium |
| **Option 2: VIP-based** | Required (etcd) | No (uses VIP) | None | Medium |
| **Option 3: Manual** | Optional | Optional | Switch traffic | Low |

---

## Option 1: Fully Automatic (Recommended)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     FULLY AUTOMATIC FAILOVER                                 │
└─────────────────────────────────────────────────────────────────────────────┘

                              Clients
                                 │
                                 ▼
                         ┌─────────────┐
                         │  HAProxy    │◄───── Health checks both nodes
                         │             │       Routes to healthy master
                         └──────┬──────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
                ▼                               ▼
         ┌─────────────┐                 ┌─────────────┐
         │   Master    │────────────────►│   Slave     │
         │   (Active)  │  Replication    │  (Standby)  │
         └──────┬──────┘                 └──────┬──────┘
                │                               │
                │         ┌─────────┐           │
                └────────►│  etcd   │◄──────────┘
                          │ (Lock)  │
                          └─────────┘

  When Master fails:
  1. Slave detects missing heartbeats
  2. Slave acquires master lock from etcd
  3. Slave promotes itself
  4. HAProxy health check detects slave is now master
  5. HAProxy routes traffic to new master

  No manual intervention required!
```

### Demo: Fully Automatic Setup

**Step 1: Create docker-compose.yml**

```yaml
version: '3.8'

services:
  # Coordination service for leader election
  etcd:
    image: quay.io/coreos/etcd:v3.5.0
    container_name: etcd
    command:
      - etcd
      - --name=etcd0
      - --advertise-client-urls=http://etcd:2379
      - --listen-client-urls=http://0.0.0.0:2379
    ports:
      - "2379:2379"
    networks:
      - stats-net

  # Master node
  master:
    image: mynadb:latest
    container_name: master
    hostname: master
    ports:
      - "7000:7000"    # HTTP API
      - "7001:7001"    # Arrow Flight
      - "7002:7002"    # Replication port
    volumes:
      - master-data:/data/stats
    environment:
      - STATS_HA_ENABLED=true
      - STATS_HA_ROLE=master
      - STATS_HA_NODE_ID=master-01
      - STATS_HA_REPLICATION_MODE=sync
      - STATS_HA_REPLICATION_LISTEN=0.0.0.0:7002
      - STATS_HA_ETCD_ENDPOINTS=etcd:2379
    networks:
      - stats-net
    depends_on:
      - etcd

  # Slave node
  slave:
    image: mynadb:latest
    container_name: slave
    hostname: slave
    ports:
      - "7010:7000"
      - "7011:7001"
    volumes:
      - slave-data:/data/stats
    environment:
      - STATS_HA_ENABLED=true
      - STATS_HA_ROLE=slave
      - STATS_HA_NODE_ID=slave-01
      - STATS_HA_MASTER_ADDR=master:7002
      - STATS_HA_FAILOVER_MODE=automatic
      - STATS_HA_ETCD_ENDPOINTS=etcd:2379
      - STATS_HA_HEARTBEAT_INTERVAL=3
      - STATS_HA_FAILURE_THRESHOLD=3
    networks:
      - stats-net
    depends_on:
      - master
      - etcd

  # Load balancer with health checks
  haproxy:
    image: haproxy:2.8
    container_name: haproxy
    ports:
      - "80:80"        # Client-facing port
      - "8404:8404"    # Stats page
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
  master-data:
  slave-data:
```

**Step 2: Create haproxy.cfg**

```
#---------------------------------------------------------------------
# HAProxy configuration for MynaDB HA
#---------------------------------------------------------------------

global
    log stdout format raw local0
    maxconn 4096

defaults
    log     global
    mode    http
    option  httplog
    option  dontlognull
    timeout connect 5000ms
    timeout client  50000ms
    timeout server  50000ms

#---------------------------------------------------------------------
# Stats page (for monitoring)
#---------------------------------------------------------------------
frontend stats
    bind *:8404
    stats enable
    stats uri /stats
    stats refresh 5s

#---------------------------------------------------------------------
# Main frontend - clients connect here
#---------------------------------------------------------------------
frontend stats_service_frontend
    bind *:80
    default_backend stats_service_backend

#---------------------------------------------------------------------
# Backend with health checks
#---------------------------------------------------------------------
backend stats_service_backend
    balance roundrobin
    option httpchk GET /ha/status

    # Check response body contains "master" role
    http-check expect string "master"

    # Master - primary server
    server master master:7000 check inter 2s fall 2 rise 2

    # Slave - backup server (only used when master fails health check)
    server slave slave:7000 check inter 2s fall 2 rise 2 backup
```

**Step 3: Start the cluster**

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps
```

**Step 4: Verify setup**

```bash
# Check master status
curl http://localhost:7000/ha/status
# Response:
# {
#   "role": "master",
#   "state": "running",
#   "node_id": "master-01",
#   "replication": {
#     "mode": "sync",
#     "slaves": [{"node_id": "slave-01", "state": "streaming", "lag_bytes": 0}]
#   }
# }

# Check slave status
curl http://localhost:7010/ha/status
# Response:
# {
#   "role": "slave",
#   "state": "streaming",
#   "node_id": "slave-01",
#   "master_addr": "master:7002",
#   "lag_bytes": 0
# }

# Check HAProxy is routing to master
curl http://localhost/ha/status
# Should return master's response

# Check HAProxy stats
open http://localhost:8404/stats
```

**Step 5: Write some data**

```bash
# Write through HAProxy (port 80)
curl -X POST "http://localhost/write/mydb" \
  -d "cpu,host=server01 usage=45.2
cpu,host=server02 usage=67.8
memory,host=server01 used=8589934592"

# Verify data on master
curl "http://localhost:7000/query/mydb" \
  -d "SELECT * FROM cpu"

# Verify data replicated to slave
curl "http://localhost:7010/query/mydb" \
  -d "SELECT * FROM cpu"
```

**Step 6: Simulate master failure**

```bash
# Kill master container
docker-compose stop master

# Watch the failover happen (check HAProxy stats or poll slave)
watch -n 1 'curl -s http://localhost:7010/ha/status | jq .role'

# After ~10 seconds, slave becomes master:
# {
#   "role": "master",
#   "state": "promoted",
#   "promoted_at": "2026-01-13T15:30:00Z",
#   "previous_role": "slave"
# }

# HAProxy automatically routes to new master
curl http://localhost/ha/status
# Returns slave (now master) response

# Writes still work!
curl -X POST "http://localhost/write/mydb" \
  -d "cpu,host=server01 usage=52.1"
```

**Failover Timeline:**

```
T=0s    Master stops sending heartbeats
T=3s    Slave misses heartbeat #1
T=6s    Slave misses heartbeat #2
T=9s    Slave misses heartbeat #3 → Failure detected!
T=9.1s  Slave tries to acquire master lock from etcd
T=9.2s  Lock acquired (old master didn't renew)
T=9.3s  Slave promotes to master
T=9.5s  HAProxy health check detects slave is now "master"
T=10s   HAProxy routes traffic to new master

Total failover time: ~10 seconds
```

**Step 7: Recover old master as slave**

```bash
# Start old master - it will detect there's a new master
docker-compose start master

# Old master becomes slave automatically
curl http://localhost:7000/ha/status
# {
#   "role": "slave",
#   "state": "syncing",
#   "master_addr": "slave:7002"
# }
```

---

## Option 2: VIP-based (No HAProxy)

Uses a **Virtual IP (VIP)** that floats between master and slave.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        VIP-BASED FAILOVER                                    │
└─────────────────────────────────────────────────────────────────────────────┘

                              Clients
                                 │
                                 │ Connect to VIP: 192.168.1.100
                                 ▼
         ┌─────────────────────────────────────────────────┐
         │                                                 │
         ▼                                                 │
  ┌─────────────┐                                         │
  │   Master    │◄─── VIP: 192.168.1.100                  │
  │ 192.168.1.1 │     (assigned to master)                │
  └──────┬──────┘                                         │
         │                                                 │
         │ Replication                                    │
         ▼                                                 │
  ┌─────────────┐                                         │
  │   Slave     │                                         │
  │ 192.168.1.2 │                                         │
  └──────┬──────┘                                         │
         │                                                 │
         │         ┌─────────┐                            │
         └────────►│  etcd   │◄───────────────────────────┘
                   └─────────┘

  When Master fails:
  1. Slave detects failure
  2. Slave acquires lock from etcd
  3. Slave assigns VIP to itself (ip addr add)
  4. Slave sends gratuitous ARP to update network
  5. Clients automatically connect to slave (same IP!)
```

**VIP Failover Script (on slave):**

```bash
#!/bin/bash
# /usr/local/bin/failover.sh

VIP="192.168.1.100"
INTERFACE="eth0"

echo "Promoting to master..."

# 1. Assign VIP to this node
ip addr add ${VIP}/24 dev ${INTERFACE}

# 2. Send gratuitous ARP to update switches/routers
arping -c 3 -A -I ${INTERFACE} ${VIP}

# 3. Notify mynadb to accept writes
curl -X POST http://localhost:7000/ha/promote

echo "Failover complete. This node is now master with VIP ${VIP}"
```

---

## Option 3: Manual Failover (Simplest)

No coordinator, no load balancer. Operator manually switches traffic.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MANUAL FAILOVER                                       │
└─────────────────────────────────────────────────────────────────────────────┘

  Normal Operation:

      Clients ──────► Master (master.example.com)
                         │
                         │ Replication
                         ▼
                      Slave (slave.example.com)

  After Manual Failover:

      Clients ──────► Slave (now master)

      DNS: master.example.com → slave's IP
      OR
      Update client config to point to slave
```

### Demo: Manual Failover

**Setup: Simple master-slave without coordinator**

```yaml
# docker-compose-simple.yml
version: '3.8'

services:
  master:
    image: mynadb:latest
    container_name: master
    ports:
      - "7000:7000"
      - "7002:7002"
    environment:
      - STATS_HA_ENABLED=true
      - STATS_HA_ROLE=master
      - STATS_HA_REPLICATION_MODE=sync
      - STATS_HA_REPLICATION_LISTEN=0.0.0.0:7002
    volumes:
      - master-data:/data/stats

  slave:
    image: mynadb:latest
    container_name: slave
    ports:
      - "7010:7000"
    environment:
      - STATS_HA_ENABLED=true
      - STATS_HA_ROLE=slave
      - STATS_HA_MASTER_ADDR=master:7002
      - STATS_HA_FAILOVER_MODE=manual
    volumes:
      - slave-data:/data/stats

volumes:
  master-data:
  slave-data:
```

**Normal operation:**

```bash
# Clients write to master
curl -X POST "http://localhost:7000/write/mydb" -d "cpu usage=42"

# Check replication status
curl http://localhost:7000/ha/status
# {"role": "master", "slaves": [{"lag_bytes": 0}]}

curl http://localhost:7010/ha/status
# {"role": "slave", "state": "streaming"}
```

**Manual failover procedure:**

```bash
#!/bin/bash
# manual_failover.sh

echo "=== Manual Failover Procedure ==="

# Step 1: Check slave is caught up
echo "Step 1: Checking replication lag..."
LAG=$(curl -s http://localhost:7010/ha/status | jq '.lag_bytes')
if [ "$LAG" != "0" ]; then
    echo "WARNING: Slave has lag of $LAG bytes. Wait or accept data loss."
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Step 2: Stop writes to master (optional, for zero data loss)
echo "Step 2: Stopping master..."
docker-compose stop master
# Or: curl -X POST http://localhost:7000/ha/pause

# Step 3: Promote slave
echo "Step 3: Promoting slave to master..."
curl -X POST http://localhost:7010/ha/promote
# Response: {"success": true, "role": "master"}

# Step 4: Verify promotion
echo "Step 4: Verifying..."
curl http://localhost:7010/ha/status
# {"role": "master", "state": "promoted"}

# Step 5: Update client configuration
echo "Step 5: Update your clients to connect to slave (port 7010)"
echo ""
echo "=== Failover Complete ==="
echo "New master is running on port 7010"
echo ""
echo "To update DNS:"
echo "  master.example.com → <slave-ip>"
echo ""
echo "Or update client config:"
echo "  stats_host = 'localhost:7010'"
```

**Run the failover:**

```bash
chmod +x manual_failover.sh
./manual_failover.sh
```

---

## Comparison Summary

| Aspect | Fully Automatic | VIP-based | Manual |
|--------|-----------------|-----------|--------|
| **Dependencies** | etcd + HAProxy | etcd + VIP | None |
| **Failover Time** | ~10 seconds | ~10 seconds | Minutes (human) |
| **Client Changes** | None | None | DNS/config update |
| **Complexity** | Medium | Medium | Low |
| **Split-Brain Risk** | Low (etcd lock) | Low (etcd lock) | None |
| **Best For** | Production | Bare metal | Dev/small setups |

## Recommendation

For production with high availability requirements:
- **Cloud/Kubernetes**: Use **Option 1** (HAProxy + etcd)
- **Bare metal**: Use **Option 2** (VIP + etcd)
- **Development/testing**: Use **Option 3** (Manual)
