# Schema Extraction Configuration

## Overview

The Stats Service supports flexible schema extraction from metric tags. This allows you to organize your metrics into different schemas (namespaces) based on custom tag values, supporting various naming patterns and hierarchical structures.

## Configuration

Schema extraction is configured in the `[schema]` section of `config.toml`:

```toml
[schema]
# Tag key to extract schema from (default: "service")
tag_key = "service"

# Default schema name when tag is not present
default_name = "default"

# Whether to prefix purely numeric schemas (e.g., "123" -> "service_123")
prefix_numeric = true

# Whether to extract the last part after dot separator
extract_last_part = false

# Custom separator for hierarchical schema names (default: ".")
separator = "."
```

## Configuration Parameters

### `tag_key` (string, default: "service")
The tag key from which to extract the schema name. Common values:
- `"service"` - Standard service identifier
- `"hierarchy_1"`, `"hierarchy_2"` - Hierarchical organization
- Any custom tag key in your metrics

### `default_name` (string, default: "default")
The schema name to use when the specified tag key is not present in the metric.

### `prefix_numeric` (boolean, default: true)
Whether to automatically prefix purely numeric schema names with "service_" to avoid SQL parser issues.

**Examples:**
- `true`: Tag `service="123"` → Schema `service_123`
- `false`: Tag `service="123"` → Schema `123`

### `extract_last_part` (boolean, default: false)
Whether to extract only the last part of hierarchical values after the separator.

**Examples:**
- `false`: Tag `service="MONITOR.web"` → Schema `MONITOR.web`
- `true`: Tag `service="MONITOR.web"` → Schema `web`

### `separator` (string, default: ".")
The separator character(s) used for hierarchical schema names. Only used when `extract_last_part = true`.

## Use Cases

### 1. Simple Service Tags

**Configuration:**
```toml
[schema]
tag_key = "service"
extract_last_part = false
prefix_numeric = true
```

**Examples:**
| Tag Value | Resulting Schema |
|-----------|------------------|
| `service="web"` | `web` |
| `service="api"` | `api` |
| `service="123"` | `service_123` |

**Use when:** You have simple, flat service identifiers.

### 2. Hierarchical Service Tags (Full Path)

**Configuration:**
```toml
[schema]
tag_key = "service"
extract_last_part = false
prefix_numeric = true
```

**Examples:**
| Tag Value | Resulting Schema |
|-----------|------------------|
| `service="MONITOR.service"` | `MONITOR.service` |
| `service="PROD.api"` | `PROD.api` |
| `service="DEV.web"` | `DEV.web` |

**Use when:** You want to preserve the full hierarchical path as the schema name.

### 3. Hierarchical Service Tags (Extract Last Part)

**Configuration:**
```toml
[schema]
tag_key = "service"
extract_last_part = true
separator = "."
prefix_numeric = true
```

**Examples:**
| Tag Value | Resulting Schema |
|-----------|------------------|
| `service="MONITOR.service"` | `service` |
| `service="PROD.api"` | `api` |
| `service="DEV.web"` | `web` |

**Use when:** Your metrics use hierarchical naming (e.g., environment.service) but you only want to organize by the service name.

### 4. Custom Tag Keys

**Configuration:**
```toml
[schema]
tag_key = "hierarchy_2"
extract_last_part = false
prefix_numeric = true
```

**Examples:**
| Tag Value | Resulting Schema |
|-----------|------------------|
| `hierarchy_2="service_1"` | `service_1` |
| `hierarchy_2="service_2"` | `service_2` |
| `hierarchy_2="123"` | `service_123` |

**Use when:** Your organization uses custom tag keys for service identification.

### 5. Multiple Separators

**Configuration:**
```toml
[schema]
tag_key = "service"
extract_last_part = true
separator = "/"
```

**Examples:**
| Tag Value | Resulting Schema |
|-----------|------------------|
| `service="prod/web"` | `web` |
| `service="dev/api"` | `api` |

**Use when:** Your service tags use a different separator (e.g., `/` or `-`).

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
tag_key = "service"
default_name = "default"
prefix_numeric = true
extract_last_part = false
```

### Changing to Hierarchical Extraction

To switch to hierarchical extraction (e.g., `MONITOR.service` → `service`):

1. Update config.toml:
```toml
[schema]
extract_last_part = true
```

2. Restart the service
3. New data will be organized with extracted schema names
4. Old data remains in the original schema directories

### Using Custom Tag Keys

To use a different tag for schema extraction:

1. Update config.toml:
```toml
[schema]
tag_key = "hierarchy_2"  # or your custom tag
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

Check that your metrics include the configured `tag_key`:
```bash
# View metrics to verify tags
curl -X POST http://localhost:7000/write/test -d 'cpu,service=web,host=server1 value=0.5'
```

### Data going to "default" schema

This happens when the `tag_key` is missing from metrics. Either:
1. Add the tag to your metrics
2. Update `default_name` to a more appropriate value

## Examples

### Example 1: Multi-Environment Setup

```toml
[schema]
tag_key = "service"
extract_last_part = true
separator = "."
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
tag_key = "service"
extract_last_part = false
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
