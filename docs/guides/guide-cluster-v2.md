# MynaDB Cluster v2 Operations Guide

This guide covers deploying and operating MynaDB Cluster v2 with the built-in Meta service.

## Quick Start

### Prerequisites

- MynaDB binary (built with `cargo build --release`)
- Shared object storage (S3, MinIO, or local filesystem)
- Network connectivity between nodes

### Minimal 3-Node Cluster

```bash
# Terminal 1: Start Meta node 1 (becomes leader)
./mynadb --role meta -c config.meta1.toml

# Terminal 2: Start Meta node 2
./mynadb --role meta -c config.meta2.toml

# Terminal 3: Start Meta node 3
./mynadb --role meta -c config.meta3.toml

# Terminal 4: Start Data node
./mynadb --role data -c config.data1.toml

# Terminal 5: Start Router node
./mynadb --role router -c config.router1.toml

# Test write (via Router)
curl -X POST http://localhost:7000/write/testdb \
  -d "cpu,host=server1 value=42.5"

# Test query
curl "http://localhost:7000/query/testdb?sql=SELECT%20*%20FROM%20cpu"
```

---

## Node Roles

### Standalone Mode (Default)

Runs all components in a single process - ideal for development and small deployments.

```bash
# Default mode - no --role flag needed
./mynadb -c config.toml
```

### Meta Node

Provides Raft-based coordination and metadata storage.

```bash
./mynadb --role meta -c config.meta.toml
```

**Requirements:**
- Odd number of nodes (3 or 5) for Raft quorum
- Persistent storage for sled database
- Low-latency network between Meta nodes

### Router Node

Handles client protocols and routes requests to Data nodes.

```bash
./mynadb --role router -c config.router.toml
```

**Characteristics:**
- Stateless - can be scaled horizontally
- Place behind a load balancer
- Caches partition→node mappings

### Data Node

Handles storage, caching, and query execution.

```bash
./mynadb --role data -c config.data.toml
```

**Requirements:**
- Local disk for WAL and cache
- Access to shared object storage
- Sufficient memory for DataCache

---

## Configuration Files

### Meta Node Configuration

```toml
# config.meta1.toml

[meta]
node_id = 1
bind_addr = "0.0.0.0:7100"
advertise_addr = "meta1:7100"
data_dir = "/data/meta"
peers = ["meta1:7100", "meta2:7100", "meta3:7100"]

[meta.raft]
heartbeat_interval_ms = 150
election_timeout_min_ms = 300
election_timeout_max_ms = 500
snapshot_threshold = 10000

[meta.events]
max_events = 100000
retention_secs = 86400

[logging]
level = "info"
log_dir = "/var/log/mynadb"
```

### Router Node Configuration

```toml
# config.router1.toml

[server]
http_port = 7000
flight_port = 7001
mysql_enabled = true
mysql_port = 7002

[meta]
endpoints = ["meta1:7100", "meta2:7100", "meta3:7100"]
connect_timeout_ms = 5000
request_timeout_ms = 30000

[routing]
cache_refresh_interval_ms = 1000
data_node_pool_size = 10

[query]
cache_enabled = true
cache_max_size_mb = 512
cache_ttl_secs = 300
max_parallel_fanout = 16

[auth]
enabled = false

[logging]
level = "info"
log_dir = "/var/log/mynadb"
```

### Data Node Configuration

```toml
# config.data1.toml

[data]
node_id = "data-1"
advertise_addr = "data1:7200"
grpc_port = 7200

[data.meta]
endpoints = ["meta1:7100", "meta2:7100", "meta3:7100"]

[storage]
data_dir = "/data/mynadb"

[storage.object_store]
backend = "s3"
bucket = "mynadb-data"
endpoint = "http://minio:9000"
region = "us-east-1"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"

[cache]
max_size_mb = 4096
flush_interval_secs = 60

[wal]
enabled = true
sync_on_write = false
rotate_size_mb = 128

[logging]
level = "info"
log_dir = "/var/log/mynadb"
```

---

## Docker Compose Deployment

### docker-compose.cluster.yaml

```yaml
version: '3.8'

services:
  # Object Storage
  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 3

  # Meta Nodes (3-node Raft cluster)
  meta1:
    image: mynadb:latest
    command: ["--role", "meta", "-c", "/config/meta1.toml"]
    ports:
      - "7100:7100"
    volumes:
      - ./config:/config:ro
      - meta1-data:/data
    depends_on:
      - minio

  meta2:
    image: mynadb:latest
    command: ["--role", "meta", "-c", "/config/meta2.toml"]
    ports:
      - "7101:7100"
    volumes:
      - ./config:/config:ro
      - meta2-data:/data
    depends_on:
      - meta1

  meta3:
    image: mynadb:latest
    command: ["--role", "meta", "-c", "/config/meta3.toml"]
    ports:
      - "7102:7100"
    volumes:
      - ./config:/config:ro
      - meta3-data:/data
    depends_on:
      - meta1

  # Data Nodes (elastic)
  data1:
    image: mynadb:latest
    command: ["--role", "data", "-c", "/config/data1.toml"]
    ports:
      - "7200:7200"
    volumes:
      - ./config:/config:ro
      - data1-cache:/data
    depends_on:
      - meta1
      - meta2
      - meta3
      - minio

  data2:
    image: mynadb:latest
    command: ["--role", "data", "-c", "/config/data2.toml"]
    ports:
      - "7201:7200"
    volumes:
      - ./config:/config:ro
      - data2-cache:/data
    depends_on:
      - meta1
      - meta2
      - meta3
      - minio

  # Router Nodes (stateless, behind LB)
  router1:
    image: mynadb:latest
    command: ["--role", "router", "-c", "/config/router1.toml"]
    ports:
      - "7000:7000"
      - "7001:7001"
      - "7002:7002"
    volumes:
      - ./config:/config:ro
    depends_on:
      - data1
      - data2

  router2:
    image: mynadb:latest
    command: ["--role", "router", "-c", "/config/router2.toml"]
    ports:
      - "7010:7000"
      - "7011:7001"
      - "7012:7002"
    volumes:
      - ./config:/config:ro
    depends_on:
      - data1
      - data2

volumes:
  minio-data:
  meta1-data:
  meta2-data:
  meta3-data:
  data1-cache:
  data2-cache:
```

### Start Cluster

```bash
# Start all services
docker-compose -f docker-compose.cluster.yaml up -d

# Check status
docker-compose -f docker-compose.cluster.yaml ps

# View logs
docker-compose -f docker-compose.cluster.yaml logs -f meta1

# Stop cluster
docker-compose -f docker-compose.cluster.yaml down
```

---

## Kubernetes Deployment

### StatefulSet for Meta Nodes

```yaml
# meta-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mynadb-meta
  namespace: mynadb
spec:
  serviceName: mynadb-meta
  replicas: 3
  selector:
    matchLabels:
      app: mynadb-meta
  template:
    metadata:
      labels:
        app: mynadb-meta
    spec:
      containers:
      - name: meta
        image: mynadb:latest
        args: ["--role", "meta", "-c", "/config/meta.toml"]
        ports:
        - containerPort: 7100
          name: grpc
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: META_NODE_ID
          value: "$(POD_NAME)"
        volumeMounts:
        - name: config
          mountPath: /config
        - name: data
          mountPath: /data
      volumes:
      - name: config
        configMap:
          name: mynadb-meta-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
---
apiVersion: v1
kind: Service
metadata:
  name: mynadb-meta
  namespace: mynadb
spec:
  clusterIP: None  # Headless service
  selector:
    app: mynadb-meta
  ports:
  - port: 7100
    name: grpc
```

### Deployment for Router Nodes

```yaml
# router-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mynadb-router
  namespace: mynadb
spec:
  replicas: 2
  selector:
    matchLabels:
      app: mynadb-router
  template:
    metadata:
      labels:
        app: mynadb-router
    spec:
      containers:
      - name: router
        image: mynadb:latest
        args: ["--role", "router", "-c", "/config/router.toml"]
        ports:
        - containerPort: 7000
          name: http
        - containerPort: 7001
          name: flight
        - containerPort: 7002
          name: mysql
        volumeMounts:
        - name: config
          mountPath: /config
      volumes:
      - name: config
        configMap:
          name: mynadb-router-config
---
apiVersion: v1
kind: Service
metadata:
  name: mynadb-router
  namespace: mynadb
spec:
  type: LoadBalancer
  selector:
    app: mynadb-router
  ports:
  - port: 7000
    name: http
  - port: 7001
    name: flight
  - port: 7002
    name: mysql
```

### StatefulSet for Data Nodes

```yaml
# data-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mynadb-data
  namespace: mynadb
spec:
  serviceName: mynadb-data
  replicas: 3
  selector:
    matchLabels:
      app: mynadb-data
  template:
    metadata:
      labels:
        app: mynadb-data
    spec:
      containers:
      - name: data
        image: mynadb:latest
        args: ["--role", "data", "-c", "/config/data.toml"]
        ports:
        - containerPort: 7200
          name: grpc
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        volumeMounts:
        - name: config
          mountPath: /config
        - name: cache
          mountPath: /data
      volumes:
      - name: config
        configMap:
          name: mynadb-data-config
  volumeClaimTemplates:
  - metadata:
      name: cache
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 100Gi
---
apiVersion: v1
kind: Service
metadata:
  name: mynadb-data
  namespace: mynadb
spec:
  clusterIP: None  # Headless service
  selector:
    app: mynadb-data
  ports:
  - port: 7200
    name: grpc
```

### Deploy to Kubernetes

```bash
# Create namespace
kubectl create namespace mynadb

# Apply ConfigMaps
kubectl apply -f configmaps.yaml -n mynadb

# Deploy Meta nodes first (wait for Raft election)
kubectl apply -f meta-statefulset.yaml -n mynadb
kubectl wait --for=condition=ready pod -l app=mynadb-meta -n mynadb --timeout=60s

# Deploy Data nodes
kubectl apply -f data-statefulset.yaml -n mynadb
kubectl wait --for=condition=ready pod -l app=mynadb-data -n mynadb --timeout=60s

# Deploy Router nodes
kubectl apply -f router-deployment.yaml -n mynadb

# Check status
kubectl get pods -n mynadb
kubectl get svc -n mynadb
```

---

## Operations

### Health Checks

```bash
# Check Router health
curl http://localhost:7000/health

# Check Data node health (via gRPC - needs grpcurl)
grpcurl -plaintext localhost:7200 datanode.DataNodeService/Health

# Check cluster status
curl http://localhost:7000/cluster/status
```

### Scaling

#### Add Router Nodes

Routers are stateless - just start more instances:

```bash
# Start another router
./mynadb --role router -c config.router3.toml

# Update load balancer to include new router
```

#### Add Data Nodes

```bash
# Start new data node
./mynadb --role data -c config.data3.toml

# Data node registers with Meta automatically
# New writes will be routed to it
```

### Monitoring

#### Key Metrics to Watch

| Metric | Alert Threshold | Description |
|--------|-----------------|-------------|
| Meta Raft leader | Changes > 1/hour | Frequent elections indicate instability |
| Data node count | < min expected | Data nodes down |
| Router response time p99 | > 500ms | Performance degradation |
| Cache hit rate | < 80% | Cache inefficiency |
| WAL size | > 1GB | Flush delays |

#### Prometheus Metrics

```yaml
# prometheus.yaml scrape config
scrape_configs:
  - job_name: 'mynadb-meta'
    static_configs:
      - targets: ['meta1:7100', 'meta2:7100', 'meta3:7100']

  - job_name: 'mynadb-router'
    static_configs:
      - targets: ['router1:7000', 'router2:7000']

  - job_name: 'mynadb-data'
    static_configs:
      - targets: ['data1:7200', 'data2:7200', 'data3:7200']
```

### Backup and Recovery

#### Meta Node Backup

The Meta state machine uses sled with periodic snapshots:

```bash
# Backup sled database
cp -r /data/meta/sled /backup/meta-$(date +%Y%m%d)

# Backup Raft log
cp -r /data/meta/raft /backup/raft-$(date +%Y%m%d)
```

#### Data Recovery

Since all data is in shared object storage, Data nodes are stateless from a durability perspective:

```bash
# If a Data node fails, just start a new one
# It will re-register with Meta and start serving

./mynadb --role data -c config.data-new.toml
```

### Troubleshooting

#### Meta Node Won't Join Cluster

1. Check network connectivity between Meta nodes
2. Verify `peers` configuration matches all nodes
3. Check sled database for corruption:
   ```bash
   ls -la /data/meta/sled/
   ```
4. Check logs for Raft errors:
   ```bash
   grep -i "raft\|election" /var/log/mynadb/meta.log
   ```

#### Router Can't Connect to Meta

1. Verify Meta endpoints in config:
   ```bash
   grep -A3 "meta" /config/router.toml
   ```
2. Test gRPC connectivity:
   ```bash
   grpcurl -plaintext meta1:7100 list
   ```
3. Check Meta leader status

#### Data Node Not Receiving Writes

1. Check registration with Meta:
   ```bash
   curl http://localhost:7000/cluster/nodes
   ```
2. Verify Data node is in ShardCache:
   ```bash
   curl http://localhost:7000/debug/shard-cache
   ```
3. Check gRPC health:
   ```bash
   grpcurl -plaintext data1:7200 datanode.DataNodeService/Health
   ```

#### Query Returns Empty Results

1. Check if data is flushed to object storage
2. Verify partition exists:
   ```bash
   aws s3 ls s3://mynadb-data/catalog/schema/table/
   ```
3. Check Meta file registry
4. Query specific Data node directly for debugging

---

## Upgrade Procedure

### Rolling Upgrade

1. **Upgrade Meta nodes one at a time:**
   ```bash
   # Stop meta3 (follower)
   systemctl stop mynadb-meta3

   # Upgrade binary
   cp mynadb-new /usr/local/bin/mynadb

   # Start meta3
   systemctl start mynadb-meta3

   # Wait for rejoin
   sleep 10

   # Repeat for meta2, then meta1 (leader last)
   ```

2. **Upgrade Data nodes:**
   ```bash
   # Drain data node
   curl -X POST http://localhost:7200/drain

   # Wait for in-flight requests
   sleep 30

   # Stop and upgrade
   systemctl stop mynadb-data1
   cp mynadb-new /usr/local/bin/mynadb
   systemctl start mynadb-data1
   ```

3. **Upgrade Router nodes:**
   ```bash
   # Remove from load balancer
   # Stop and upgrade
   # Add back to load balancer
   ```

### Version Compatibility

| Component | Compatible Versions |
|-----------|-------------------|
| Meta ↔ Meta | Same major version |
| Router ↔ Meta | ±1 minor version |
| Router ↔ Data | ±1 minor version |
| Data ↔ Meta | ±1 minor version |

---

## Performance Tuning

### Meta Node

```toml
[meta.raft]
# Faster elections for lower latency failover
heartbeat_interval_ms = 100
election_timeout_min_ms = 200
election_timeout_max_ms = 400

# More frequent snapshots for faster recovery
snapshot_threshold = 5000
```

### Router Node

```toml
[routing]
# Larger connection pool for high throughput
data_node_pool_size = 20

# More aggressive cache refresh
cache_refresh_interval_ms = 500

[query]
# Higher fanout for complex queries
max_parallel_fanout = 32
```

### Data Node

```toml
[cache]
# Larger cache for better hit rate
max_size_mb = 8192

# Less frequent flush for better batching
flush_interval_secs = 120

[wal]
# Async writes for higher throughput
sync_on_write = false
```

---

## Security (Future)

### TLS Configuration

```toml
# Coming soon
[tls]
enabled = true
cert_file = "/certs/server.crt"
key_file = "/certs/server.key"
ca_file = "/certs/ca.crt"
```

### Authentication

```toml
[auth]
enabled = true
username = "admin"
password = "secret"
```

---

## API Reference

### Write Data

```bash
# Single line
curl -X POST http://router:7000/write/mydb \
  -d "cpu,host=server1 value=42.5"

# Multiple lines
curl -X POST http://router:7000/write/mydb \
  -d "cpu,host=server1 value=42.5
memory,host=server1 used=1024
disk,host=server1 free=50.2"
```

### Query Data

```bash
# SQL query
curl "http://router:7000/query/mydb?sql=SELECT%20*%20FROM%20cpu%20LIMIT%2010"

# With time range
curl "http://router:7000/query/mydb?sql=SELECT%20*%20FROM%20cpu%20WHERE%20time%20%3E%20now()%20-%20interval%20%271%20hour%27"
```

### Cluster Status

```bash
# Get cluster status
curl http://router:7000/cluster/status

# List nodes
curl http://router:7000/cluster/nodes

# Get leader info
curl http://router:7000/cluster/leader
```

### Flush Data

```bash
# Flush all cached data
curl -X POST http://router:7000/flush
```
