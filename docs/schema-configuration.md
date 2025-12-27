# Schema Configuration

## Overview

The Stats Service organizes data using DataFusion's catalog/schema/table hierarchy:

```
catalog = database (from URI path)
schema  = service (from configured tag, defaults to "default")
table   = measurement (from line protocol)
```

## Configuration

Configure schema extraction in `config.toml`:

```toml
[schema]
# Tags to try for schema extraction (in order)
tag_keys = ["MONITOR.service", "hierarchy_2", "service"]

# Default schema when no tag matches
default_name = "default"

# Prefix numeric schemas (e.g., "123" -> "svc_123")
prefix_numeric = true
```

## Examples

### Write Data
```bash
# With service tag -> schema "webapp"
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,service=webapp,host=server01 usage=75.5'

# With numeric service -> schema "svc_1"
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,service=1,host=server01 usage=75.5'

# No service tag -> schema "default"
curl -X POST http://localhost:7000/write/mydb \
  -d 'memory,host=server01 usage=85.5'
```

### File Structure
```
data/
└── mydb/                      # catalog
    ├── webapp/                # schema (from service=webapp)
    │   └── cpu/               # table
    │       └── date=2024-01-15/
    │           └── L0_*.parquet
    ├── svc_1/                 # schema (from service=1, prefixed)
    │   └── cpu/
    └── default/               # schema (no service tag)
        └── memory/
```

### Query Data

```sql
-- Fully qualified
SELECT * FROM mydb.webapp.cpu

-- Schema.table (auto-prefixes catalog)
SELECT * FROM webapp.cpu

-- Simple name (auto-prefixes catalog.default.)
SELECT * FROM memory

-- Cross-schema join
SELECT a.*, b.*
FROM mydb.webapp.cpu a
JOIN mydb.svc_1.cpu b ON a.host = b.host
```

## Schema Naming Rules

| Tag Value | `prefix_numeric` | Resulting Schema |
|-----------|------------------|------------------|
| `service="web"` | any | `web` |
| `service="api"` | any | `api` |
| `service="123"` | `true` | `svc_123` |
| `service="123"` | `false` | `123` |
| (no tag) | any | `default` |

## Multiple Tag Keys

The `tag_keys` array is checked in order:

```toml
[schema]
tag_keys = ["MONITOR.service", "hierarchy_2", "service"]
```

For a metric with tags `hierarchy_2=myapp`, the schema becomes `myapp`.

## Troubleshooting

### Data goes to "default" schema
- Check that your metrics include one of the configured `tag_keys`
- Verify `tag_keys` order in config.toml

### SQL errors with numeric schemas
- Enable `prefix_numeric = true` in config.toml
- Numeric schemas like "123" become "svc_123"

### Query returns empty results
- Use fully qualified names: `mydb.schema.table`
- Check schema name with: `ls data/mydb/`
