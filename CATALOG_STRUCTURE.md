# Catalog/Schema/Table Structure

## Overview

The write logic now fully utilizes DataFusion's catalog/schema/table architecture for better organization and querying.

## Structure Mapping

```
catalog = database (from URI path)
schema  = service (from "service" tag, defaults to "default")
table   = measurement (from line protocol)
```

## Example Request

```bash
curl -X POST http://localhost:7000/write/mydb -d 'cpu,arch=x86,service=1,hostname=host_001 cnt=1i 1766534400000000000'
```

This creates:
- **Catalog**: `mydb` (from URI `/write/mydb`)
- **Schema**: `1` (from `service=1` tag)
- **Table**: `cpu` (measurement name)

File structure: `data/mydb/1/cpu/L0_<timestamp>.parquet`

## Default Schema

If no `service` tag is present, data goes to the `default` schema:

```bash
curl -X POST http://localhost:7000/write/mydb -d 'memory,host=server01 usage=85.5'
```

Creates: `data/mydb/default/memory/`

## Querying

### Fully Qualified Names

```sql
SELECT * FROM mydb.1.cpu WHERE hostname = 'host_001'
SELECT * FROM mydb.default.memory WHERE host = 'server01'
```

### Auto-Prefixed (Simple Names)

When querying via `/query/mydb`, table names are auto-prefixed with `mydb.default.`:

```bash
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM cpu LIMIT 10'
# Executes: SELECT * FROM mydb.default.cpu LIMIT 10
```

### Cross-Schema Joins

```sql
SELECT 
  a.hostname, 
  a.cnt as service1_count,
  b.cnt as service2_count
FROM mydb.1.cpu a
JOIN mydb.2.cpu b ON a.hostname = b.hostname
```

## Benefits

1. **Service Isolation**: Different services can have the same measurement names without conflict
2. **Flexible Querying**: Use fully qualified names or simple names with auto-prefixing
3. **DataFusion Native**: Leverages DataFusion's catalog system instead of flat namespace
4. **Better Organization**: Data organized by catalog → schema → table hierarchy
5. **High Performance**: Batch operations, minimal lock contention

## Key Changes

### Files Modified

- `src/query_engine.rs`: Catalog/schema/table registration and discovery
- `src/cache.rs`: Updated to use (catalog, schema, table) cache keys
- `src/parquet_writer.rs`: Writes to `catalog/schema/table/` paths
- `src/main.rs`: Extracts schema from `service` tag, registers catalog.schema

### Performance Optimizations

- Batch point insertion by (schema, measurement)
- Single registration per catalog.schema pair
- Auto-discovery scans filesystem only once per catalog
- In-memory tracking of registered catalogs/schemas
