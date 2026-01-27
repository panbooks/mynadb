# MynaDB Kubernetes Deployment

This directory contains Kubernetes manifests for deploying MynaDB with warm standby replication.

## Prerequisites

1. Kubernetes cluster (1.21+)
2. kubectl configured
3. S3-compatible object storage (AWS S3, MinIO, etc.)
4. At least 3 nodes across 3 availability zones (recommended)

## Quick Start

### 1. Create Namespace

```bash
kubectl create namespace mynadb
```

### 2. Configure Secrets

```bash
cp secrets-template.yaml secrets.yaml
# Edit secrets.yaml with your S3 credentials
kubectl apply -f secrets.yaml -n mynadb
```

### 3. Deploy

```bash
kubectl apply -k . -n mynadb
```

### 4. Verify

```bash
kubectl get pods -n mynadb
```

### 5. Access

```bash
kubectl port-forward svc/mynadb-router 7000:7000 -n mynadb

# Write data
curl -X POST 'http://localhost:7000/write/mydb' -d 'cpu,host=server01 value=0.64'

# Query data
curl 'http://localhost:7000/query/mydb?sql=SELECT%20*%20FROM%20cpu'
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MYNADB_NODE_ID` | Stable node identifier | Pod name |
| `MYNADB_ADVERTISE_ADDR` | Address for peer communication | DNS from headless service |
| `MYNADB_META_ENDPOINTS` | Meta service endpoints | ConfigMap |
| `MYNADB_ZONE` | Availability zone | K8s node label |
| `MYNADB_REPLICATION_FACTOR` | Default replication factor | 2 |

## Scaling

```bash
# Scale data nodes (triggers automatic rebalancing)
kubectl scale statefulset mynadb-data --replicas=5 -n mynadb
```

## Warm Standby Failover

- **Instant (Router-driven)**: ~150ms - Router routes to Standby on write failure
- **Official (Meta-driven)**: ~10s - Meta promotes Standby to Leader
