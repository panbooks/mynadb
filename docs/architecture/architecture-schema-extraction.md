# Schema Extraction Configuration

## Overview

MynaDB supports flexible schema extraction from metric tags. This allows you to organize your metrics into different schemas (namespaces) based on custom tag values, supporting various naming patterns and hierarchical structures.

## Configuration

Schema extraction is configured in the `[schema]` section of `config.toml`:

```toml
[schema]
# Strategy: "service_as_schema" (default) or "service_as_table"
strategy = "service_as_schema"

# Tag keys to extract schema from (checked in order, first match wins)
tag_keys = ["MONITOR.service", "hierarchy_2", "service"]

# Default schema name when no matching tag is present
default_name = "default"

# Whether to prefix purely numeric schemas (e.g., "123" -> "service_123")
prefix_numeric = true
```

## Configuration Parameters

### `strategy` (string, default: "service_as_schema")
How to map the extracted value:
- `"service_as_schema"` - Tag value becomes the schema name, measurement becomes the table name
- `"service_as_table"` - Tag value becomes the table name, schema is always "default"

### `tag_keys` (array of strings, default: `["service"]`)
An ordered list of tag keys to check for schema extraction. The first matching key in the metric is used. Common values:
- `["service"]` - Standard service identifier
- `["MONITOR.service", "hierarchy_2", "service"]` - Multiple fallback keys
- Any custom tag keys in your metrics

### `default_name` (string, default: "default")
The schema name to use when none of the `tag_keys` are present in the metric.

### `prefix_numeric` (boolean, default: true)
Whether to automatically prefix purely numeric schema names with "service_" to avoid SQL parser issues.

**Examples:**
- `true`: Tag `service="123"` → Schema `service_123`
- `false`: Tag `service="123"` → Schema `123`

## Use Cases

### 1. Simple Service Tags

**Configuration:**
```toml
[schema]
tag_keys = ["service"]
prefix_numeric = true
```

**Examples:**
| Tag Value | Resulting Schema |
|-----------|------------------|
| `service="web"` | `web` |
| `service="api"` | `api` |
| `service="123"` | `service_123` |

**Use when:** You have simple, flat service identifiers.

### 2. Hierarchical Service Tags

**Configuration:**
```toml
[schema]
tag_keys = ["service"]
prefix_numeric = true
```

**Examples:**
| Tag Value | Resulting Schema |
|-----------|------------------|
| `service="MONITOR.service"` | `MONITOR.service` |
| `service="PROD.api"` | `PROD.api` |
| `service="DEV.web"` | `DEV.web` |

**Use when:** You want to preserve the full tag value as the schema name.

### 3. Multiple Fallback Tag Keys

**Configuration:**
```toml
[schema]
tag_keys = ["MONITOR.service", "hierarchy_2", "service"]
prefix_numeric = true
```

**Examples:**
- Metric with `MONITOR.service="web"` → Schema `web` (first key matched)
- Metric with `hierarchy_2="svc1"` → Schema `svc1` (second key matched)
- Metric with `service="api"` → Schema `api` (third key matched)
- Metric with no matching tags → Schema `default`

**Use when:** Your system has metrics with different tag naming conventions.

### 4. Custom Tag Keys

**Configuration:**
```toml
[schema]
tag_keys = ["hierarchy_2"]
prefix_numeric = true
```

**Examples:**
| Tag Value | Resulting Schema |
|-----------|------------------|
| `hierarchy_2="service_1"` | `service_1` |
| `hierarchy_2="service_2"` | `service_2` |
| `hierarchy_2="123"` | `service_123` |

**Use when:** Your organization uses custom tag keys for service identification.

### 5. ServiceAsTable Strategy

**Configuration:**
```toml
[schema]
strategy = "service_as_table"
tag_keys = ["service"]
```

**Examples:**
- `service="web"` → Schema `default`, Table `web`
- `service="api"` → Schema `default`, Table `api`

**Use when:** You want a flat table namespace instead of nested schemas.

## Data Organization

With schema extraction, your data is organized as:

```
data/
├── benchmark/                # Catalog (database)
│   ├── default/             # Schema (extracted from tags)
│   │   └── cpu/            # Table (measurement)
│   │       └── date=2025-12-25/
│   ├── web/                # Schema
│   │   └── response_time/
│   └── api/                # Schema
│       └── requests/
└── _system/
    └── default/
        └── series_stats/
```

## Querying with Schemas

When querying data, use the schema name in your SQL:

```sql
-- Query from "web" schema
SELECT * FROM benchmark.web.cpu LIMIT 10;

-- Query from "api" schema
SELECT * FROM benchmark.api.requests WHERE time > now() - interval '1 hour';

-- Query from hierarchical schema (full path preserved)
SELECT * FROM benchmark."MONITOR.service".metrics LIMIT 10;
```

## Migration Guide

### From Previous Version (Static "service" Tag)

If you were using the default behavior before, no changes are needed. The default configuration maintains backward compatibility:

```toml
[schema]
tag_keys = ["service"]
default_name = "default"
prefix_numeric = true
```

### Using Multiple Tag Keys

To support multiple naming conventions:

1. Update config.toml:
```toml
[schema]
tag_keys = ["MONITOR.service", "hierarchy_2", "service"]
```

2. Restart the service
3. Tags are checked in order; first match wins
4. Old data remains in the original schema directories

### Using Custom Tag Keys

To use a different tag for schema extraction:

1. Update config.toml:
```toml
[schema]
tag_keys = ["hierarchy_2"]  # or your custom tag
```

2. Ensure your metrics include this tag
3. Restart the service

## Best Practices

1. **Consistent Naming**: Use a consistent schema naming convention across your metrics
2. **Avoid Special Characters**: Schema names become directory names - avoid special characters
3. **Plan for Growth**: Choose a schema strategy that scales with your organization
4. **Test Configuration**: Test schema extraction with sample data before deploying to production
5. **Document Schemas**: Maintain documentation of your schema organization

## Troubleshooting

### Schema names with dots are causing issues

If `extract_last_part = false` and you have dots in service names:
- SQL queries may need quotes: `SELECT * FROM benchmark."MONITOR.service".cpu`
- Consider setting `extract_last_part = true` to simplify schema names

### Numeric schemas causing SQL errors

Enable `prefix_numeric = true` to automatically prefix numeric schemas:
```toml
[schema]
prefix_numeric = true  # "123" becomes "service_123"
```

### Wrong tag being used for schema

Check that your metrics include one of the configured `tag_keys`:
```bash
# View metrics to verify tags
curl -X POST http://localhost:7000/write/test -d 'cpu,service=web,host=server1 value=0.5'
```

### Data going to "default" schema

This happens when none of the `tag_keys` are present in metrics. Either:
1. Add one of the configured tags to your metrics
2. Update `default_name` to a more appropriate value

## Examples

### Example 1: Multi-Environment Setup

```toml
[schema]
tag_keys = ["service"]
```

Write data:
```bash
curl -X POST http://localhost:7000/write/prod -d 'cpu,service=PROD.web,host=server1 value=0.5'
curl -X POST http://localhost:7000/write/prod -d 'cpu,service=PROD.api,host=server2 value=0.7'
curl -X POST http://localhost:7000/write/dev -d 'cpu,service=DEV.web,host=dev1 value=0.3'
```

Data organization:
```
data/prod/web/cpu/
data/prod/api/cpu/
data/dev/web/cpu/
```

### Example 2: Service-Based Organization

```toml
[schema]
tag_keys = ["service"]
```

Write data:
```bash
curl -X POST http://localhost:7000/write/metrics -d 'response_time,service=auth,endpoint=/login value=125'
curl -X POST http://localhost:7000/write/metrics -d 'response_time,service=payment,endpoint=/checkout value=250'
```

Data organization:
```
data/metrics/auth/response_time/
data/metrics/payment/response_time/
```

## See Also

- [CONFIG.md](CONFIG.md) - Complete configuration reference
- [CATALOG_STRUCTURE.md](CATALOG_STRUCTURE.md) - Data organization guide
- [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - Quick reference guide
