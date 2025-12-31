# Schema Extraction - Quick Reference

## Quick Start

Add to your `config.toml`:

```toml
[schema]
tag_key = "service"           # Tag to extract schema from
default_name = "default"      # Fallback when tag is missing
prefix_numeric = true         # "123" → "service_123"
extract_last_part = false     # "A.B" → "A.B" (false) or "B" (true)
separator = "."               # Separator for hierarchical names
```

## Common Configurations

### 1. Simple Service Tag (Default)
```toml
[schema]
tag_key = "service"
extract_last_part = false
```
✅ `service="web"` → `web`  
✅ `service="api"` → `api`  
✅ `service="123"` → `service_123`

### 2. Hierarchical (Extract Last)
```toml
[schema]
tag_key = "service"
extract_last_part = true
```
✅ `service="MONITOR.web"` → `web`  
✅ `service="PROD.api"` → `api`

### 3. Hierarchical (Keep Full)
```toml
[schema]
tag_key = "service"
extract_last_part = false
```
✅ `service="MONITOR.web"` → `MONITOR.web`  
✅ `service="PROD.api"` → `PROD.api`

### 4. Custom Tag Key
```toml
[schema]
tag_key = "hierarchy_2"
extract_last_part = false
```
✅ `hierarchy_2="svc1"` → `svc1`  
✅ `hierarchy_2="svc2"` → `svc2`

## Testing Your Configuration

```bash
# 1. Write test data
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,service=web,host=server1 value=0.5'

# 2. List schemas
curl http://localhost:7000/databases/test/schemas

# 3. Query data
curl "http://localhost:7000/query/test?sql=SELECT+*+FROM+web.cpu+LIMIT+10"

# 4. Check data directory
ls -la ./data/test/
```

## Compaction with Schemas

```bash
# Compact specific schema.table
curl -X POST http://localhost:7000/compact/CATALOG/SCHEMA/TABLE

# Examples:
curl -X POST http://localhost:7000/compact/benchmark/web/cpu
curl -X POST http://localhost:7000/compact/benchmark/api/requests
curl -X POST http://localhost:7000/compact/_system/default/series_stats
```

## Common Patterns

| Use Case | Configuration | Example |
|----------|---------------|---------|
| Simple services | `extract_last_part=false` | `web`, `api` |
| Env.Service format | `extract_last_part=true` | `PROD.web` → `web` |
| Custom hierarchy | `tag_key="hierarchy_2"` | Custom tag |
| Numeric IDs | `prefix_numeric=true` | `123` → `service_123` |
| Path separators | `separator="/"` | `prod/web` → `web` |

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Data in "default" schema | Check tag is present in metrics |
| SQL errors with schema names | Enable `prefix_numeric=true` |
| Wrong schema extracted | Verify `tag_key` matches your tags |
| Dots causing issues | Set `extract_last_part=true` or use quotes in SQL |

## Need More Info?

- 📖 Full guide: [SCHEMA_EXTRACTION.md](SCHEMA_EXTRACTION.md)
- ⚙️ All options: [CONFIG.md](CONFIG.md)
- 📝 Changes: [SCHEMA_CONFIG_CHANGES.md](SCHEMA_CONFIG_CHANGES.md)
