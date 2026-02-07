# Getting Started with MinIO

MinIO provides S3-compatible object storage for MynaDB. This guide covers installing MinIO, creating buckets, and running MynaDB in each mode with MinIO as the storage backend.

## 1. Install MinIO

### Binary (Linux amd64)

```bash
curl -O https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
sudo mv minio /usr/local/bin/
```

### Docker

```bash
docker pull minio/minio
```

### Install MinIO Client (mc)

```bash
curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/
```

## 2. Start MinIO

### Binary

```bash
# Start with a local data directory
minio server /tmp/minio-data --console-address ":9001"
```

### Docker

```bash
docker run -d --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -v /tmp/minio-data:/data \
  minio/minio server /data --console-address ":9001"
```

This starts:

| Service       | URL                       |
|---------------|---------------------------|
| S3 API        | http://localhost:9000      |
| Web Console   | http://localhost:9001      |

Default credentials: `minioadmin` / `minioadmin`

### Verify MinIO is running

```bash
curl -s http://127.0.0.1:9000/minio/health/live
# Should return 200 OK
```

## 3. Create Buckets

### Using mc

```bash
mc alias set myminio http://127.0.0.1:9000 minioadmin minioadmin

# For standalone mode
mc mb myminio/mynadb-standalone

# For cold standby cluster
mc mb myminio/mynadb-coldstandby

# For cold standby simple cluster
mc mb myminio/mynadb-coldstandby-simple
```

### Using the Web Console

1. Open http://localhost:9001
2. Log in with `minioadmin` / `minioadmin`
3. Go to **Buckets** > **Create Bucket**
4. Enter the bucket name and create

## 4. Storage Configuration

The `[storage]` section in config TOML files controls the storage backend:

```toml
[storage]
backend = "minio"                       # Storage backend type
data_dir = "./dev/data/standalone"      # Local directory for WAL, cache, temp files
bucket = "mynadb-standalone"            # MinIO bucket for parquet files
region = "us-east-1"                    # S3 region (required but arbitrary for MinIO)
access_key = "minioadmin"               # MinIO access key
secret_key = "minioadmin"               # MinIO secret key
endpoint = "http://127.0.0.1:9000"     # MinIO S3 API endpoint
allow_http = true                       # Required for non-TLS connections
```

**Key distinction:**
- `data_dir` — local filesystem path for WAL logs, write cache, and temporary files
- `bucket` — remote MinIO/S3 bucket where parquet data files are persisted

The data directory structure inside MinIO follows the Hive partition layout:
```
bucket/
└── {catalog}/
    └── {schema}/
        └── {table}/
            └── date=YYYY-MM-DD/
                └── node_id={node_id}/
                    ├── L0_xxx.parquet
                    └── L1_xxx.parquet
```

## 5. Running MynaDB with MinIO

### Standalone Mode

The simplest way — single MynaDB node with MinIO storage:

```bash
# Config: dev/configs/standalone/minio.toml
# Bucket: mynadb-standalone
# Ports:  HTTP 7000, Flight 7001, MySQL 7002

./dev/start-standalone.sh minio
```

Or manually:
```bash
goreman -f dev/Procfile.standalone-minio start
```

### Cold Standby Simple (1 meta + 4 data + 1 router)

Minimal cluster for testing failover:

```bash
# Bucket: mynadb-coldstandby-simple
# Ports:  Meta 6000/6001, Router 7000-7002, Data 5000-5301

./dev/start-coldstandby-simple.sh
```

| Node   | Role    | HTTP | Flight | Zone   |
|--------|---------|------|--------|--------|
| meta1  | Meta    | 6000 | 6001   | -      |
| router | Router  | 7000 | 7001   | -      |
| data1  | Data    | 5000 | 5001   | zone-a |
| data2  | Data    | 5100 | 5101   | zone-b |
| data3  | Data    | 5200 | 5201   | zone-a |
| data4  | Data    | 5300 | 5301   | zone-b |

### Cold Standby Full (3 meta + 6 data + 1 router + 1 admin)

Full cluster with Raft consensus, 6 data nodes across 3 zones, and admin UI:

```bash
# Bucket: mynadb-coldstandby
# Admin UI: http://localhost:18080/admin

./dev/start-coldstandby.sh
```

| Node   | Role    | HTTP  | Flight | Zone   |
|--------|---------|-------|--------|--------|
| meta1  | Meta    | 6000  | 6001   | -      |
| meta2  | Meta    | 6100  | 6101   | -      |
| meta3  | Meta    | 6200  | 6201   | -      |
| router | Router  | 7000  | 7001   | -      |
| data1  | Data    | 5000  | 5001   | zone-a |
| data2  | Data    | 5100  | 5101   | zone-b |
| data3  | Data    | 5200  | 5201   | zone-c |
| data4  | Data    | 5300  | 5301   | zone-a |
| data5  | Data    | 5400  | 5401   | zone-b |
| data6  | Data    | 5500  | 5501   | zone-c |
| admin  | Admin   | 18080 | -      | -      |

## 6. Quick Smoke Test

After the cluster is running:

```bash
# Write data
curl -X POST http://localhost:7000/write/testdb \
  -d 'cpu,service=web,host=server1 value=0.5
cpu,service=web,host=server2 value=0.8'

# Flush cache to MinIO
curl -X POST http://localhost:7000/flush

# Query
curl 'http://localhost:7000/query/testdb?sql=SELECT+*+FROM+cpu'

# Check partitions on a data node
curl http://localhost:5000/partitions/testdb/web/cpu

# Browse in MinIO console
# http://localhost:9001 → Buckets → mynadb-*
```

## 7. Browsing MinIO Data

### mc commands

```bash
# List all objects in a bucket
mc ls myminio/mynadb-coldstandby --recursive

# List partition directories for a table
mc ls myminio/mynadb-coldstandby/testdb/web/cpu/

# Download a parquet file for inspection
mc cp myminio/mynadb-coldstandby/testdb/web/cpu/date=2026-02-07/node_id=data-1/L0_xxx.parquet /tmp/

# Get bucket disk usage
mc du myminio/mynadb-coldstandby
```

### Web Console

Open http://localhost:9001, navigate to the bucket, and browse the `catalog/schema/table/date=.../node_id=.../` hierarchy visually.

## 8. Clean Up

```bash
# Remove all objects from a bucket
mc rm --recursive --force myminio/mynadb-coldstandby

# Remove the bucket
mc rb myminio/mynadb-coldstandby

# Stop MinIO (Docker)
docker stop minio && docker rm minio

# Clean local data directories
rm -rf dev/data/*/
rm -rf dev/logs/*/
```

## 9. Troubleshooting

### "MinIO is not running on port 9000"

The start scripts check for MinIO before launching. Make sure MinIO is started first:

```bash
minio server /tmp/minio-data --console-address ":9001"
```

### "Bucket does not exist"

Create the bucket before starting MynaDB:

```bash
mc alias set myminio http://127.0.0.1:9000 minioadmin minioadmin
mc mb myminio/mynadb-standalone
```

### Partitions show empty in storage browser

After writing data, it stays in the write cache until flushed. Force a flush:

```bash
curl -X POST http://localhost:7000/flush
```

### "Connection refused" from admin to data nodes

Check that `http_proxy` / `HTTP_PROXY` environment variables are not set — they can interfere with admin-to-data-node requests:

```bash
unset http_proxy HTTP_PROXY https_proxy HTTPS_PROXY
```

### Custom MinIO credentials

Update all relevant config files under `dev/configs/` — each data node config has its own `[storage]` section with `access_key` and `secret_key`.
