# Schema Configuration Update Summary

## Changes Made

### 1. Configuration Structure (`src/config.rs`)

**Changed `tag_key` from single String to `tag_keys` array:**

```rust
// Before
pub tag_key: String,

// After
pub tag_keys: Vec<String>,
```

**Removed configurations:**
- `extract_last_part: bool` - ❌ Removed
- `separator: String` - ❌ Removed

**Final SchemaConfig structure:**
```rust
pub struct SchemaConfig {
    pub tag_keys: Vec<String>,      // Array of tag keys to try
    pub default_name: String,       // Fallback schema name
    pub prefix_numeric: bool,       // Prefix numeric schemas
}
```

### 2. Schema Extraction Logic (`src/cache.rs`)

**New behavior:**
- Tries each tag key in the `tag_keys` array in order
- Uses the first matching tag value found
- Falls back to `default_name` if none match

```rust
pub fn extract_schema(&self, tags: &HashMap<String, String>) -> String {
    // Try each tag key in order until we find one
    for tag_key in &self.schema_config.tag_keys {
        if let Some(value) = tags.get(tag_key) {
            if self.schema_config.prefix_numeric && value.chars().all(|c| c.is_numeric()) {
                return format!("service_{}", value);
            } else {
                return value.clone();
            }
        }
    }
    
    // No matching tag found, use default
    self.schema_config.default_name.clone()
}
```

### 3. Configuration File (`config.toml`)

**Before:**
```toml
[schema]
tag_key = "service"
default_name = "default"
prefix_numeric = true
extract_last_part = false
separator = "."
```

**After:**
```toml
[schema]
tag_keys = ["service"]
default_name = "default"
prefix_numeric = true
```

## Usage Examples

### Example 1: Single Tag Key (Default)
```toml
[schema]
tag_keys = ["service"]
```
```bash
# Metric with service tag
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,service=web,host=server1 value=0.5'
# Result: data/test/web/cpu/
```

### Example 2: Multiple Tag Keys (Priority Order)
```toml
[schema]
tag_keys = ["service", "hierarchy_2", "app_name"]
```

**Priority logic:**
1. First tries `service` tag
2. If not found, tries `hierarchy_2` tag
3. If not found, tries `app_name` tag
4. If none found, uses `default_name`

```bash
# Has service tag - uses it
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,service=web,hierarchy_2=svc1 value=0.5'
# Result: data/test/web/cpu/ (uses "service" tag)

# No service tag - uses hierarchy_2
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,hierarchy_2=svc1,host=server1 value=0.5'
# Result: data/test/svc1/cpu/ (uses "hierarchy_2" tag)

# No matching tags - uses default
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,host=server1 value=0.5'
# Result: data/test/default/cpu/ (uses default_name)
```

### Example 3: Hierarchical Tags
```toml
[schema]
tag_keys = ["service", "hierarchy_2"]
```

**Important:** Values are used as-is (no extraction):
```bash
# Value "MONITOR.service" is used directly
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,service=MONITOR.service,host=server1 value=0.5'
# Result: data/test/MONITOR.service/cpu/
```

### Example 4: Numeric Tags
```toml
[schema]
tag_keys = ["service"]
prefix_numeric = true
```
```bash
# Numeric value gets prefixed
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,service=123,host=server1 value=0.5'
# Result: data/test/service_123/cpu/
```

## Compaction Examples

```bash
# Compact with simple schema
curl -X POST http://localhost:7000/compact/benchmark/web/cpu

# Compact with hierarchical schema (value used as-is)
curl -X POST http://localhost:7000/compact/benchmark/MONITOR.service/cpu

# Compact non-partitioned
curl -X POST http://localhost:7000/compact/_system/default/series_stats
```

## Key Differences from Previous Version

| Feature | Before | After |
|---------|--------|-------|
| Tag key type | Single string | Array of strings |
| Tag matching | One specific tag | Multiple tags (priority order) |
| Hierarchical extraction | Configurable with `extract_last_part` | Not supported (use value as-is) |
| Separator config | Configurable | Not supported |

## Migration from Previous Configuration

**If your old config was:**
```toml
[schema]
tag_key = "service"
extract_last_part = false
separator = "."
```

**Change to:**
```toml
[schema]
tag_keys = ["service"]
```

**If you were using multiple tag keys in different contexts:**
```toml
[schema]
tag_keys = ["service", "hierarchy_2", "app_name"]
```

## Testing

```bash
# 1. Test with service tag
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,service=web,host=server1 value=0.5'

# 2. Test with hierarchy_2 tag (if configured)
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,hierarchy_2=svc1,host=server1 value=0.5'

# 3. Test without matching tags (should use default)
curl -X POST http://localhost:7000/write/test \
  -d 'cpu,host=server1 value=0.5'

# 4. List schemas
curl http://localhost:7000/databases/test/schemas

# 5. Check data directory
ls -la ./data/test/
```

## Summary

✅ **Simplified configuration** - Removed `extract_last_part` and `separator`  
✅ **More flexible** - Support multiple tag keys with priority  
✅ **Cleaner logic** - Tag values used directly without processing  
✅ **Priority-based** - First matching tag wins  

**Note:** Tag values are now used directly as schema names. If you need to extract parts from hierarchical values (e.g., "MONITOR.service" → "service"), this should be done at the data ingestion layer before sending to the stats service.
