# MynaDB Cluster Port Conventions

This document describes the standardized port allocation for MynaDB cluster deployments.

## Port Layout Overview

```
┌─────────────────────────────────────────────────────────────┐
│  ROUTER (Client Gateway)                                    │
│    router-1:  HTTP=7000  Flight=7001  MySQL=7002           │
├─────────────────────────────────────────────────────────────┤
│  META (Raft Consensus)                                      │
│    meta-1:    HTTP=6000  gRPC=6001                         │
│    meta-2:    HTTP=6100  gRPC=6101                         │
│    meta-3:    HTTP=6200  gRPC=6201                         │
├─────────────────────────────────────────────────────────────┤
│  DATA (Storage & Query)                                     │
│    data-1:    HTTP=5000  Flight=5001                       │
│    data-2:    HTTP=5100  Flight=5101                       │
└─────────────────────────────────────────────────────────────┘
```

## Port Formula

Each node type uses a **base port** with consistent **offsets** for each protocol:

```
Node Type   Base    HTTP(+0)  Protocol(+1)  Protocol(+2)
---------   ----    --------  ------------  ------------
Data-N      5000    +0        Flight +1     -
Meta-N      6000    +0        gRPC +1       -
Router-N    7000    +0        Flight +1     MySQL +2
```

**For multiple nodes of the same type**, increment the base by 100:

```
Meta Node N:   HTTP = 6000 + (N-1)*100,  gRPC = 6001 + (N-1)*100
Data Node N:   HTTP = 5000 + (N-1)*100,  Flight = 5001 + (N-1)*100
Router Node N: HTTP = 7000 + (N-1)*100,  Flight = +1,  MySQL = +2
```

## Node Types

### Meta Nodes (2 ports)

Meta nodes run the Raft consensus protocol for cluster coordination.

| Offset | Protocol | Purpose |
|--------|----------|---------|
| +0     | HTTP     | Health checks, metrics, management API |
| +1     | gRPC     | Raft consensus (inter-node communication) |

**Configuration example (`meta1.toml`):**
```toml
[meta]
node_id = 1
http_port = 6000                    # Base + 0
bind_addr = "0.0.0.0:6001"          # Base + 1 (gRPC)
advertise_addr = "127.0.0.1:6001"
peers = ["127.0.0.1:6101", "127.0.0.1:6201"]  # Other meta gRPC ports
```

### Data Nodes (2 ports)

Data nodes handle storage (Parquet files) and query execution.

| Offset | Protocol | Purpose |
|--------|----------|---------|
| +0     | HTTP     | Health checks, metrics, flush, compact APIs |
| +1     | Flight   | Arrow Flight (query data from Router) |

**Configuration example (`data1.toml`):**
```toml
[data]
node_id = "data-1"
http_port = 5000                    # Base + 0
advertise_addr = "127.0.0.1:5001"   # Base + 1 (Flight)
meta_endpoints = ["127.0.0.1:6001", "127.0.0.1:6101", "127.0.0.1:6201"]
```

### Router Nodes (3 ports)

Router nodes are client-facing gateways that handle writes and route queries.

| Offset | Protocol | Purpose |
|--------|----------|---------|
| +0     | HTTP     | Write API, Query API, health, management |
| +1     | Flight   | Arrow Flight queries (client connections) |
| +2     | MySQL    | MySQL wire protocol (client connections) |

**Configuration example (`router1.toml`):**
```toml
[router]
node_id = "router-1"
http_port = 7000    # Base + 0
flight_port = 7001  # Base + 1
mysql_port = 7002   # Base + 2

[router.meta]
endpoints = ["127.0.0.1:6001", "127.0.0.1:6101", "127.0.0.1:6201"]
```

## Inter-Node Communication

### Data/Router → Meta

Data and Router nodes connect to Meta nodes via **gRPC** (port offset +1):

```
meta_endpoints = ["127.0.0.1:6001", "127.0.0.1:6101", "127.0.0.1:6201"]
```

### Router → Data

Router nodes connect to Data nodes via **Arrow Flight** using the `advertise_addr` registered in Meta:

```
Data-1 advertises: 127.0.0.1:5001 (Flight)
Data-2 advertises: 127.0.0.1:5101 (Flight)
```

### Meta ↔ Meta (Raft)

Meta nodes communicate with each other via **gRPC** for Raft consensus:

```
# meta1.toml
peers = ["127.0.0.1:6101", "127.0.0.1:6201"]

# meta2.toml
peers = ["127.0.0.1:6001", "127.0.0.1:6201"]

# meta3.toml
peers = ["127.0.0.1:6001", "127.0.0.1:6101"]
```

## Default Ports (Standalone Mode)

When running MynaDB in standalone (non-cluster) mode:

| Port | Protocol | Purpose |
|------|----------|---------|
| 7000 | HTTP     | Write/Query API |
| 7001 | Flight   | Arrow Flight |
| 7002 | MySQL    | MySQL wire protocol |

## Verification

After starting the cluster, verify ports are listening:

```bash
# Check all cluster ports
ss -tlnp | grep -E '(5[0-9]{3}|6[0-9]{3}|7[0-9]{3})'

# Test health endpoints
curl http://localhost:6000/health   # Meta-1 HTTP
curl http://localhost:6100/health   # Meta-2 HTTP
curl http://localhost:6200/health   # Meta-3 HTTP
curl http://localhost:5000/health   # Data-1 HTTP
curl http://localhost:5100/health   # Data-2 HTTP
curl http://localhost:7000/health   # Router HTTP
```

## Kubernetes Considerations

In Kubernetes deployments, each pod typically exposes the same container ports, with service discovery handling routing:

```yaml
# Data node container ports
ports:
  - name: http
    containerPort: 5000
  - name: flight
    containerPort: 5001

# Meta node container ports
ports:
  - name: http
    containerPort: 6000
  - name: grpc
    containerPort: 6001
```

Use headless services for stable DNS names:
- `mynadb-meta-0.mynadb-meta-headless:6001`
- `mynadb-data-0.mynadb-data-headless:5001`
