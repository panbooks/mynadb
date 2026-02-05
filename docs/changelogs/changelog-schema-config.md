# Schema Configuration Enhancement - Change Summary

## Overview

Enhanced the schema extraction logic to be fully configurable, supporting various service tag formats including simple values, hierarchical names (e.g., "MONITOR.service"), and custom tag keys (e.g., "hierarchy_2").

## Changes Made

### 1. Configuration Structure (`src/config.rs`)

**Added new `SchemaConfig` struct:**
```rust
pub struct SchemaConfig {
    pub tag_key: String,           // Tag key to extract schema from (default: "service")
    pub default_name: String,      // Default schema name when tag is absent
    pub prefix_numeric: bool,      // Whether to prefix numeric schemas
    pub extract_last_part: bool,   // Whether to extract last part after separator
    pub separator: String,         // Separator for hierarchical names
}
```

**Updated `Config` struct to include schema configuration:**
```rust
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub schema: SchemaConfig,      // NEW
    pub cache: CacheConfig,
    pub compaction: CompactionConfig,
    pub series_tracker: SeriesTrackerConfig,
    pub logging: LoggingConfig,
}
```

### 2. Data Cache (`src/cache.rs`)

**Updated `DataCache` to use schema configuration:**
```rust
pub struct DataCache {
    cache: Arc<Mutex<HashMap<CacheKey, Vec<RecordBatch>>>>,
    writer: Arc<ParquetWriter>,
    schema_config: Arc<SchemaConfig>,  // NEW
}
```

**Enhanced `extract_schema` method:**
- Changed from static method to instance method
- Now uses configurable tag key (not hardcoded "service")
- Supports hierarchical extraction (e.g., "MONITOR.service" → "service")
- Supports custom separators
- Maintains backward compatibility

**Added features:**
```rust
pub fn extract_schema(&self, tags: &HashMap<String, String>) -> String {
    tags.get(&self.schema_config.tag_key)
        .map(|s| {
            let mut schema = s.clone();
            
            // Extract last part if hierarchical
            if self.schema_config.extract_last_part 
                && schema.contains(&self.schema_config.separator) {
                if let Some(last_part) = schema.split(&self.schema_config.separator).last() {
                    schema = last_part.to_string();
                }
            }
            
            // Prefix numeric schemas
            if self.schema_config.prefix_numeric && schema.chars().all(|c| c.is_numeric()) {
                format!("service_{}", schema)
            } else {
                schema
            }
        })
        .unwrap_or_else(|| self.schema_config.default_name.clone())
}
```

### 3. Main Application (`src/main.rs`)

**Updated DataCache initialization:**
```rust
// Before
let cache = DataCache::new(&config.storage.data_dir);

// After
let cache = DataCache::new(&config.storage.data_dir, Arc::new(config.schema.clone()));
```

**Updated schema extraction in write handler:**
```rust
// Before
let schema = DataCache::extract_schema(&point.tags);

// After
let schema = state.cache.extract_schema(&point.tags);
```

### 4. Configuration Files

**Updated `config.toml`:**
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

**Updated `config.toml.example`:**
- Added comprehensive schema section with examples and comments

### 5. Documentation

**Updated `CONFIG.md`:**
- Added detailed [schema] section with parameters table
- Added 4 use case examples
- Explained configuration for different scenarios

**Created `SCHEMA_EXTRACTION.md`:**
- Comprehensive guide on schema extraction
- Configuration examples for 5 common use cases
- Migration guide from previous version
- Best practices and troubleshooting
- Query examples

## Usage Examples

### Example 1: Simple Service Tags
```toml
[schema]
tag_key = "service"
extract_last_part = false
```
- `service="web"` → Schema: `web`
- `service="api"` → Schema: `api`

### Example 2: Hierarchical Tags (Extract Last Part)
```toml
[schema]
tag_key = "service"
extract_last_part = true
separator = "."
```
- `service="MONITOR.service"` → Schema: `service`
- `service="PROD.api"` → Schema: `api`

### Example 3: Custom Tag Keys
```toml
[schema]
tag_key = "hierarchy_2"
extract_last_part = false
```
- `hierarchy_2="service_1"` → Schema: `service_1`
- `hierarchy_2="service_2"` → Schema: `service_2`

### Example 4: Numeric Service IDs
```toml
[schema]
tag_key = "service"
prefix_numeric = true
```
- `service="123"` → Schema: `service_123` (avoids SQL parser issues)

## Backward Compatibility

✅ **Fully backward compatible**

The default configuration maintains the original behavior:
```toml
[schema]
tag_key = "service"
default_name = "default"
prefix_numeric = true
extract_last_part = false
separator = "."
```

Existing deployments will work without any changes.

## Testing

### Test Cases to Verify

1. **Simple service tag:**
   ```bash
   curl -X POST http://localhost:7000/write/test -d 'cpu,service=web value=0.5'
   # Expected: data/test/web/cpu/
   ```

2. **Hierarchical tag (full path):**
   ```bash
   # With extract_last_part=false
   curl -X POST http://localhost:7000/write/test -d 'cpu,service=MONITOR.web value=0.5'
   # Expected: data/test/MONITOR.web/cpu/
   ```

3. **Hierarchical tag (extract last):**
   ```bash
   # With extract_last_part=true
   curl -X POST http://localhost:7000/write/test -d 'cpu,service=MONITOR.web value=0.5'
   # Expected: data/test/web/cpu/
   ```

4. **Numeric service:**
   ```bash
   curl -X POST http://localhost:7000/write/test -d 'cpu,service=123 value=0.5'
   # Expected: data/test/service_123/cpu/
   ```

5. **Custom tag key:**
   ```bash
   # With tag_key="hierarchy_2"
   curl -X POST http://localhost:7000/write/test -d 'cpu,hierarchy_2=my_service value=0.5'
   # Expected: data/test/my_service/cpu/
   ```

6. **Missing tag (default):**
   ```bash
   curl -X POST http://localhost:7000/write/test -d 'cpu,host=server1 value=0.5'
   # Expected: data/test/default/cpu/
   ```

## Files Modified

1. `src/config.rs` - Added SchemaConfig struct and integration
2. `src/cache.rs` - Enhanced extract_schema method with configuration support
3. `src/main.rs` - Updated DataCache initialization and usage
4. `config.toml` - Added [schema] section
5. `config.toml.example` - Added [schema] section with examples
6. `CONFIG.md` - Added schema configuration documentation

## Files Created

1. `SCHEMA_EXTRACTION.md` - Comprehensive schema extraction guide
2. `SCHEMA_CONFIG_CHANGES.md` - This file (change summary)

## Benefits

1. **Flexibility**: Support for any tag key, not just "service"
2. **Hierarchical Support**: Handle dot-separated values like "MONITOR.service"
3. **Customizable**: All extraction logic is configurable
4. **Backward Compatible**: Existing deployments work without changes
5. **Well Documented**: Comprehensive documentation with examples

## Next Steps

1. Test the changes with real data
2. Verify compilation: `cargo build --release`
3. Run integration tests
4. Update any deployment scripts if needed
5. Communicate changes to users

## API Examples for Testing

```bash
# 1. Test default behavior
curl -X POST http://localhost:7000/write/benchmark -d 'cpu,service=web,host=server1 value=0.5'

# 2. Test hierarchical service tag
curl -X POST http://localhost:7000/write/benchmark -d 'cpu,service=MONITOR.web,host=server1 value=0.6'

# 3. Test numeric service
curl -X POST http://localhost:7000/write/benchmark -d 'cpu,service=123,host=server1 value=0.7'

# 4. Test missing service tag (should use default)
curl -X POST http://localhost:7000/write/benchmark -d 'cpu,host=server1 value=0.8'

# 5. Query the data
curl "http://localhost:7000/query/benchmark?sql=SELECT+*+FROM+web.cpu+LIMIT+10"

# 6. List schemas
curl http://localhost:7000/databases/benchmark/schemas

# 7. Compact with schema names
curl -X POST http://localhost:7000/compact/benchmark/web/cpu
```

## Configuration Examples for Different Scenarios

### Scenario 1: Multi-Tenant SaaS (Extract Environment)
```toml
[schema]
tag_key = "service"
extract_last_part = true
separator = "."
# PROD.customer1 -> customer1, DEV.customer2 -> customer2
```

### Scenario 2: Microservices with Namespaces
```toml
[schema]
tag_key = "service"
extract_last_part = false
separator = "."
# Keep full path: monitoring.api, logging.service
```

### Scenario 3: Legacy System with Custom Tags
```toml
[schema]
tag_key = "hierarchy_2"
extract_last_part = false
prefix_numeric = true
# Use existing hierarchy_2 tag
```

## Compaction Examples (From User's Original Query)

The compaction commands work the same way, but now respect the schema configuration:

```bash
# Compact series_stats (non-partitioned, system schema)
curl -X POST http://localhost:7000/compact/_system/default/series_stats

# Compact regular metrics (partitioned, using extracted schema)
# If service="web", then:
curl -X POST http://localhost:7000/compact/benchmark/web/cpu

# If service="MONITOR.web" with extract_last_part=true, then:
curl -X POST http://localhost:7000/compact/benchmark/web/cpu

# If service="MONITOR.web" with extract_last_part=false, then:
curl -X POST 'http://localhost:7000/compact/benchmark/MONITOR.web/cpu'
```
