# Fixes Applied - Catalog/Schema/Table Issues

## Issues Found and Fixed

### Issue 1: Schema Type Mismatch
**Error**: `Arrow: Incompatible type. Field 'hostname' has type Utf8, array has type Int64`

**Root Cause**: When multiple RecordBatches with different schemas were added to the cache, they had incompatible field types.

**Fix**: Added schema merging and batch adaptation in `parquet_writer.rs`:
- `merge_schemas()` - Merges schemas from all batches, ensuring compatibility
- `adapt_batch_to_schema()` - Adapts batches to match the merged schema
- Fills missing fields with null arrays

```rust
// Before: Used first batch schema (incompatible with other batches)
let arrow_schema = batches[0].schema();

// After: Merge all batch schemas
let merged_schema = Self::merge_schemas(&batches)?;
```

### Issue 2: SQL Parser Error with Numeric Schemas
**Error**: `SQL error: ParserError("Expected end of statement, found: .1")`

**Root Cause**: DataFusion's SQL parser doesn't support numeric identifiers after dots (e.g., `mydb.1.cpu` is invalid).

**Fix**: Modified `cache.rs` to prefix numeric service tags with `"service_"`:

```rust
pub fn extract_schema(tags: &HashMap<String, String>) -> String {
    tags.get("service")
        .map(|s| {
            // If service value is purely numeric, prefix with "service_"
            if s.chars().all(|c| c.is_numeric()) {
                format!("service_{}", s)
            } else {
                s.clone()
            }
        })
        .unwrap_or_else(|| DEFAULT_SCHEMA.to_string())
}
```

**Schema Mapping**:
- `service=1` → `service_1` ✅
- `service=2` → `service_2` ✅
- `service=production` → `production` ✅
- No service tag → `default` ✅

### Issue 3: Table Not Found Error
**Error**: `table 'mydb.default.memory' not found`

**Root Cause**: The query engine wasn't handling partially qualified table names (schema.table) correctly.

**Fix**: Enhanced `prefix_table_names()` in `query_engine.rs` to support:
1. Simple names: `cpu` → `mydb.default.cpu`
2. Partially qualified: `service_1.cpu` → `mydb.service_1.cpu`
3. Fully qualified: `mydb.service_1.cpu` → unchanged

```rust
let dot_count = table_ref.matches('.').count();

let prefixed = if dot_count == 0 {
    // Simple name: table -> catalog.default.table
    format!("{}.{}.{}", catalog_name, DEFAULT_SCHEMA, table_ref)
} else if dot_count == 1 {
    // Partially qualified: schema.table -> catalog.schema.table
    format!("{}.{}", catalog_name, table_ref)
} else {
    // Already fully qualified
    table_ref.to_string()
};
```

## Files Modified

### 1. `src/parquet_writer.rs`
- Added `merge_schemas()` to combine schemas from multiple batches
- Added `adapt_batch_to_schema()` to adapt batches to merged schema
- Updated `write_batches()` to use merged schema

### 2. `src/cache.rs`
- Modified `extract_schema()` to prefix numeric service tags with "service_"

### 3. `src/query_engine.rs`
- Enhanced `prefix_table_names()` to handle 3 qualification levels
- Support for simple, partially qualified, and fully qualified names

### 4. Documentation
- Updated `QUICK_REFERENCE.md` with schema naming rules
- Updated `test_write.sh` with corrected schema names

## Expected Directory Structure

```
data/
└── mydb/
    ├── service_1/           # service=1 (numeric → prefixed)
    │   ├── cpu/
    │   └── disk/
    ├── service_2/           # service=2 (numeric → prefixed)
    │   └── cpu/
    ├── production/      # service=production (text → as-is)
    │   └── cpu/
    └── default/         # no service tag
        └── memory/
```

## Query Examples (All Valid Now)

```sql
-- Fully qualified
SELECT * FROM mydb.service_1.cpu
SELECT * FROM mydb.service_2.cpu
SELECT * FROM mydb.production.cpu
SELECT * FROM mydb.default.memory

-- Partially qualified (auto-prefixed with catalog)
SELECT * FROM service_1.cpu
SELECT * FROM production.cpu

-- Simple names (auto-prefixed with catalog.default)
SELECT * FROM cpu
SELECT * FROM memory

-- Cross-schema joins
SELECT * FROM mydb.service_1.cpu a JOIN mydb.service_2.cpu b ON a.hostname = b.hostname
```

## Testing

Clean the old data and restart:
```bash
rm -rf data/mydb
cargo run --release
```

Run the updated test script:
```bash
chmod +x test_write.sh
./test_write.sh
```

All tests should now pass without errors!
