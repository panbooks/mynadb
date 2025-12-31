# Quick Reference - Catalog/Schema/Table Structure

## Schema Naming

**Important**: Numeric service tags are automatically prefixed with `svc_` to avoid SQL parser issues.

- `service=1` → schema `svc_1`
- `service=2` → schema `svc_2`
- `service=production` → schema `production` (no prefix needed)
- No service tag → schema `default`

## Write Data

```bash
# With numeric service tag (creates schema "svc_1")
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,arch=x86,service=1,hostname=host_001 cnt=1i 1766534400000000000'

# With text service tag (creates schema "production")
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,service=production,hostname=host_001 cnt=1i'

# Without service tag (uses "default" schema)
curl -X POST http://localhost:7000/write/mydb \
  -d 'memory,host=server01 usage=85.5'
```

## File Structure Created

```
data/
└── mydb/                    # catalog (from URI /write/mydb)
    ├── svc_1/               # schema (from service=1 tag)
    │   └── cpu/             # table (measurement name)
    │       └── L0_*.parquet
    ├── svc_2/               # schema (from service=2 tag)
    │   └── cpu/
    │       └── L0_*.parquet
    ├── production/          # schema (from service=production tag)
    │   └── cpu/
    │       └── L0_*.parquet
    └── default/             # schema (no service tag)
        └── memory/
            └── L0_*.parquet
```

## Query Data

### Fully Qualified Names
```bash
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.svc_1.cpu'
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.production.cpu'
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM mydb.default.memory'
```

### Partially Qualified (schema.table)
```bash
# Automatically prefixed with catalog name
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM svc_1.cpu'
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM production.cpu'
```

### Auto-Prefixed (Simple Names)
```bash
# Automatically prefixed with "mydb.default."
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM cpu'
curl 'http://localhost:7000/query/mydb' -d 'SELECT * FROM memory'
```

### Cross-Schema Queries
```bash
curl 'http://localhost:7000/query/mydb' -d 'SELECT 
  a.hostname, a.cnt as cnt1, b.cnt as cnt2 
FROM mydb.svc_1.cpu a 
JOIN mydb.svc_2.cpu b ON a.hostname = b.hostname'
```

## Mapping

| Line Protocol | Becomes | Example |
|--------------|---------|---------|
| URI path `/write/{db}` | Catalog | `mydb` |
| Tag `service=1` | Schema | `svc_1` (numeric → prefixed) |
| Tag `service=production` | Schema | `production` (text → as-is) |
| No `service` tag | Schema | `default` |
| Measurement | Table | `cpu`, `memory`, `disk` |

## DataFusion CLI

```bash
datafusion-cli --host localhost --port 7001

# In datafusion-cli:
> SELECT * FROM mydb.svc_1.cpu;
> SELECT * FROM mydb.svc_2.cpu;
> SELECT * FROM mydb.production.cpu;
> SELECT COUNT(*) FROM mydb.default.memory;
```

## Performance Notes

- Points are grouped by `(schema, measurement)` before writing
- One batch operation per `(catalog, schema, table)` group
- Schema evolution supported (batches with different fields are merged)
- Minimal lock contention
- Auto-registration on first write to new schema
- Auto-discovery on first query to catalog
