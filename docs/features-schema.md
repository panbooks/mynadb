# 🎯 Configurable Schema Extraction

## What's New

The Stats Service now supports **fully configurable schema extraction** from metric tags! You can now customize how schemas (namespaces) are organized based on your tag structure.

## Quick Overview

### Before (Hardcoded)
```rust
// Only supported service="web" format
schema = tags.get("service")  // Hardcoded "service" tag
```

### After (Configurable)
```toml
[schema]
tag_key = "service"           # Or "hierarchy_2", "app_name", etc.
extract_last_part = true      # Handle "MONITOR.service" → "service"
prefix_numeric = true         # "123" → "service_123"
```

## Supported Formats

| Format | Example | Configuration | Result |
|--------|---------|---------------|--------|
| Simple | `service="web"` | Default | `web` |
| Hierarchical | `service="MONITOR.web"` | `extract_last_part=true` | `web` |
| Full Path | `service="MONITOR.web"` | `extract_last_part=false` | `MONITOR.web` |
| Custom Tag | `hierarchy_2="svc1"` | `tag_key="hierarchy_2"` | `svc1` |
| Numeric | `service="123"` | `prefix_numeric=true` | `service_123` |

## Configuration

Add to your `config.toml`:

```toml
[schema]
# Which tag to use for schema extraction
tag_key = "service"

# Fallback when tag is missing
default_name = "default"

# Prefix numeric values to avoid SQL issues
prefix_numeric = true

# Extract last part of hierarchical values
# false: "MONITOR.service" → "MONITOR.service"
# true:  "MONITOR.service" → "service"
extract_last_part = false

# Separator for hierarchical extraction
separator = "."
```

## Common Use Cases

### 1. Default (Simple Services)
```toml
[schema]
tag_key = "service"
extract_last_part = false
```
Perfect for: Microservices, simple deployments

### 2. Environment.Service Format
```toml
[schema]
tag_key = "service"
extract_last_part = true
```
Perfect for: Multi-environment setups, isolating by service

### 3. Custom Tag Keys
```toml
[schema]
tag_key = "hierarchy_2"
```
Perfect for: Legacy systems, custom naming conventions

## Examples

### Write Data
```bash
# Simple format
curl -X POST http://localhost:7000/write/prod \
  -d 'cpu,service=web,host=server1 value=0.5'

# Hierarchical format
curl -X POST http://localhost:7000/write/prod \
  -d 'cpu,service=MONITOR.web,host=server1 value=0.5'

# Custom tag
curl -X POST http://localhost:7000/write/prod \
  -d 'cpu,hierarchy_2=my_service,host=server1 value=0.5'
```

### Compaction
```bash
# Works with any schema format
curl -X POST http://localhost:7000/compact/CATALOG/SCHEMA/TABLE

# Examples:
curl -X POST http://localhost:7000/compact/prod/web/cpu
curl -X POST http://localhost:7000/compact/_system/default/series_stats
```

## Data Organization

Your data structure adapts to the configuration:

```
data/
├── prod/                    # Catalog
│   ├── web/                # Schema (from service tag)
│   │   ├── cpu/           # Table
│   │   └── memory/
│   └── api/                # Another schema
│       └── requests/
└── _system/
    └── default/
        └── series_stats/
```

## Migration

✅ **Fully backward compatible!**

If you don't add `[schema]` to your config, everything works exactly as before. No breaking changes.

## Documentation

| Document | Description |
|----------|-------------|
| [SCHEMA_EXTRACTION.md](SCHEMA_EXTRACTION.md) | Complete guide with examples |
| [SCHEMA_QUICK_REF.md](SCHEMA_QUICK_REF.md) | Quick reference card |
| [MIGRATION_SCHEMA_CONFIG.md](MIGRATION_SCHEMA_CONFIG.md) | How to migrate |
| [config.examples.toml](config.examples.toml) | 10 example configurations |
| [CONFIG.md](CONFIG.md) | Full config reference |

## Benefits

🎯 **Flexible** - Use any tag key, not just "service"  
🌳 **Hierarchical** - Handle "MONITOR.service" format  
🔧 **Configurable** - All behavior can be customized  
↩️ **Compatible** - No breaking changes  
📚 **Documented** - Comprehensive guides included  

## Try It Now

1. **Add config:**
```toml
[schema]
extract_last_part = true  # Enable hierarchical extraction
```

2. **Restart service:**
```bash
./stats
```

3. **Test it:**
```bash
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,service=MONITOR.web,host=test value=0.5'

curl http://localhost:7000/databases/test/schemas
# Should show: ["web"]
```

## Questions?

- 📖 Read the [full guide](SCHEMA_EXTRACTION.md)
- ⚡ Check the [quick reference](SCHEMA_QUICK_REF.md)
- 🔍 See [example configs](config.examples.toml)
- 🚀 Follow the [migration guide](MIGRATION_SCHEMA_CONFIG.md)

---

**This feature answers the original request:**
> "the service schema can be 'service' or 'MONITOR.service', or 'hierarchy_2'  
> please update the source code, it is better to be configured"

✅ All requirements met! The schema extraction is now fully configurable.
