# Guide: Distributed Write Routing

This guide explains how to configure write distribution across multiple primary nodes for better load balancing.

## Overview

By default, MynaDB routes all writes for a table to a single primary node (table-level affinity). This provides excellent cache locality but can create hot spots when a few tables receive most of the write traffic.

**Distributed Write Routing** allows you to spread writes for a table across multiple (or all) primary nodes:

- **percent=0**: Single node routing (default)
- **percent=50**: ~50% of primary nodes receive writes
- **percent=100**: All primary nodes receive writes

## Architecture

```
                    ┌─────────────┐
                    │   Router    │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌─────────┐  ┌─────────┐  ┌─────────┐
        │ Primary │  │ Primary │  │ Primary │
        │  Node 1 │  │  Node 2 │  │  Node 3 │
        └─────────┘  └─────────┘  └─────────┘

With distribution=100%, writes go to ALL primary nodes
With distribution=50%, writes go to ~50% of nodes (random selection)
With distribution=0%, writes go to ONE node (table affinity)
```

## Configuration Hierarchy

Distribution config follows a hierarchical lookup order:

1. **Table-level** (most specific): `catalog/schema/table`
2. **Schema-level**: `catalog/schema`
3. **Catalog-level** (least specific): `catalog`

Example: If you set 100% distribution at catalog level, all tables in that catalog will be distributed unless overridden at schema or table level.

## API Reference

### Set Distribution Config

```bash
# Distribute all writes in 'mydb' catalog to all primary nodes
curl -X POST http://meta:7101/distribution/config \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": "mydb",
    "schema": "",
    "table": "",
    "percent": 100
  }'

# Response
{
  "success": true,
  "catalog": "mydb",
  "schema": "",
  "table": "",
  "scope": "catalog",
  "percent": 100,
  "version": 1
}
```

### Set Schema-Level Config

```bash
# Distribute writes for 'mydb/metrics' schema to 50% of nodes
curl -X POST http://meta:7101/distribution/config \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": "mydb",
    "schema": "metrics",
    "table": "",
    "percent": 50
  }'
```

### Set Table-Level Config

```bash
# Disable distribution for a specific hot table (single node)
curl -X POST http://meta:7101/distribution/config \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": "mydb",
    "schema": "metrics",
    "table": "cpu",
    "percent": 0
  }'
```

### Get Effective Config

```bash
# Get the effective config for a specific table (hierarchical lookup)
curl "http://meta:7101/distribution/config?catalog=mydb&schema=metrics&table=cpu&effective=true"

# Response
{
  "catalog": "mydb",
  "schema": "metrics",
  "table": "cpu",
  "scope": "table",
  "percent": 0,
  "created_at": 1706889600000,
  "last_modified": 1706889600000,
  "version": 1
}
```

### Get Exact Config

```bash
# Get config for exact key (no hierarchical lookup)
curl "http://meta:7101/distribution/config?catalog=mydb&schema=&table="

# Response (catalog-level config)
{
  "catalog": "mydb",
  "schema": "",
  "table": "",
  "scope": "catalog",
  "percent": 100,
  ...
}
```

### List All Configs

```bash
# List all distribution configs
curl "http://meta:7101/distribution"

# List configs for a specific catalog
curl "http://meta:7101/distribution?catalog=mydb"
```

### Delete Config

```bash
# Delete a distribution config
curl -X DELETE http://meta:7101/distribution/config \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": "mydb",
    "schema": "",
    "table": "",
    "percent": 0
  }'
```

## How It Works

### Write Path

1. **Router receives write request**
2. **Check distribution config** (from local cache, not Meta - for performance)
3. **Select target nodes**:
   - `percent=0`: Use table-level affinity (single node)
   - `percent>0`: Select N nodes where N = ceil(total_nodes * percent / 100)
4. **Write to all selected nodes in parallel**
5. **Register all successful nodes as table owners in Meta**

### Query Path

1. **Router receives query request**
2. **Lookup table owners from Meta** (all nodes that have data)
3. **Scatter query to all owner nodes**
4. **Gather and aggregate results**

### Cache Invalidation

- Distribution configs are cached locally in Router's ShardCache
- Meta publishes `DistributionConfigChanged` events on config changes
- Router subscribes to events and updates local cache automatically
- No Meta query needed on the hot write path

## Performance Considerations

### When to Use Distribution

**Good use cases:**
- High-cardinality tables with many concurrent writers
- Tables receiving >100K writes/second
- When you want to maximize aggregate write throughput

**Avoid distribution for:**
- Tables with mostly sequential access patterns
- Small tables that fit in single node's cache
- Tables where query locality matters more than write throughput

### Trade-offs

| Aspect | Single Node (0%) | Distributed (100%) |
|--------|------------------|-------------------|
| Write throughput | Limited by 1 node | Scales with nodes |
| Query performance | Excellent (local) | Scatter-gather overhead |
| Cache efficiency | Optimal | Reduced (data spread) |
| Complexity | Simple | Higher |

## Example: High-Throughput Metrics

```bash
# 1. Create a catalog for high-throughput metrics
# (writes distributed across all nodes by default)
curl -X POST http://meta:7101/distribution/config \
  -d '{"catalog":"metrics","schema":"","table":"","percent":100}'

# 2. Override for a specific table that needs locality
curl -X POST http://meta:7101/distribution/config \
  -d '{"catalog":"metrics","schema":"default","table":"slow_queries","percent":0}'

# 3. Verify effective config
curl "http://meta:7101/distribution/config?catalog=metrics&schema=default&table=cpu&effective=true"
# Returns: percent=100 (inherited from catalog)

curl "http://meta:7101/distribution/config?catalog=metrics&schema=default&table=slow_queries&effective=true"
# Returns: percent=0 (table-level override)
```

## Monitoring

Check cache stats to verify distribution config caching:

```bash
curl http://router:7000/cache/stats
```

Response includes `distribution_config_count` showing how many configs are cached.

## Troubleshooting

### Writes not being distributed

1. **Check config exists**: `GET /distribution/config?effective=true`
2. **Check cache invalidation**: Verify Router is subscribed to Meta events
3. **Check node availability**: Only active primary nodes receive distributed writes

### Query returns partial results

1. **Verify all owner nodes are healthy**: `GET /nodes?role=data&state=active`
2. **Check scatter-gather logs**: Look for "scatter-gather query" in Router logs
3. **Verify table owners in Meta**: Query should go to all nodes with data

### High latency on distributed writes

1. **Check network between nodes**: Distributed writes have higher latency
2. **Consider reducing percent**: 50% distribution may be sufficient
3. **Monitor individual node performance**: One slow node affects all writes
