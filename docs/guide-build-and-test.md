# Build and Test Instructions

## Build

```bash
cd /root/projects/stats-service
cargo build --release
```

## Run Server

```bash
cargo run --release
```

The server will start on:
- HTTP: `http://localhost:7000`
- Arrow Flight: `grpc://localhost:7001`

## Test the New Catalog Structure

### 1. Write data with different services

```bash
# Service 1
curl -X POST http://localhost:7000/write/mydb -d 'cpu,arch=x86,datacenter=ap-southeast-1b,hostname=host_9991227,os=Ubuntu15.10,rack=62,region=ap-southeast-1,service=1,service_environment=production,service_version=1,team=SF cnt=1i 1766534400000000000'

# Service 2
curl -X POST http://localhost:7000/write/mydb -d 'cpu,arch=x86,datacenter=ap-southeast-1b,hostname=host_9991228,os=Ubuntu15.10,rack=62,region=ap-southeast-1,service=2,service_environment=production,service_version=1,team=SF cnt=2i 1766534401000000000'

# No service tag (goes to 'default' schema)
curl -X POST http://localhost:7000/write/mydb -d 'memory,host=server01 usage=85.5 1766534402000000000'
```

### 2. Verify file structure

```bash
tree data/mydb
# Should show:
# data/mydb/
# ├── 1/
# │   └── cpu/
# │       └── L0_*.parquet
# ├── 2/
# │   └── cpu/
# │       └── L0_*.parquet
# └── default/
#     └── memory/
#         └── L0_*.parquet
```

### 3. Query data

```bash
# Query service 1
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb.1.cpu'

# Query service 2
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb.2.cpu'

# Query default schema with auto-prefix (simple name)
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM memory'
# Auto-expanded to: SELECT * FROM mydb.default.memory

# Cross-service join
curl 'http://localhost:7000/query/mydb?sql=SELECT a.service, a.cnt as cnt1, b.cnt as cnt2 FROM mydb.1.cpu a JOIN mydb.2.cpu b ON a.hostname = b.hostname'
```

### 4. Use datafusion-cli (Arrow Flight)

```bash
datafusion-cli --host localhost --port 7001

# In datafusion-cli:
> SELECT * FROM mydb.1.cpu;
> SELECT * FROM mydb.2.cpu;
> SELECT * FROM mydb.default.memory;
```

## Expected Behavior

1. **Write**: Data is organized into `catalog/schema/table/` directory structure
2. **Schema Extraction**: `service` tag value becomes the schema name
3. **Default Schema**: Points without `service` tag go to `default` schema
4. **Auto-Registration**: Catalogs and schemas are registered on-demand during writes
5. **Auto-Discovery**: Tables are discovered from filesystem on first query
6. **Query Flexibility**: Support both fully qualified names and auto-prefixed simple names

## Performance Notes

- Batch operations minimize lock contention
- Single registration per catalog.schema.table
- Discovery only happens once per catalog on first query
- No overhead for subsequent queries to same catalog
