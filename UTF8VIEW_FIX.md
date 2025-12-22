# UTF8View Type Support Fix

## Problem

When querying data, string columns were showing as "Unsupported type: Utf8View":

```json
{
  "host": "Unsupported type: Utf8View",
  "measurement": "Unsupported type: Utf8View",
  "timestamp": 1766412971150078267,
  "usage": 23.5
}
```

## Root Cause

**DataFusion/Arrow Evolution:**
- Newer versions of DataFusion use `Utf8View` for string data
- `Utf8View` is a more memory-efficient string representation
- Our JSON conversion code only supported the older `Utf8` type

## String Types in Arrow

Arrow has multiple string types:

1. **`Utf8`** (String) - Traditional variable-length strings
    - Each string stored separately
    - Offset array + data buffer

2. **`Utf8View`** (StringView) - New, more efficient
    - Strings ≤12 bytes stored inline
    - Strings >12 bytes stored in separate buffer
    - Better cache locality and memory usage

3. **`LargeUtf8`** (LargeString) - For very large strings
    - Uses 64-bit offsets instead of 32-bit
    - For strings >2GB

## The Fix

Updated `query_engine.rs` to handle all three string types:

```rust
match array.data_type() {
    // ... other types ...
    
    DataType::Utf8 => {
        let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
        Ok(serde_json::json!(arr.value(index)))
    }
    DataType::Utf8View => {  // NEW!
        let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
        Ok(serde_json::json!(arr.value(index)))
    }
    DataType::LargeUtf8 => {  // NEW!
        let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
        Ok(serde_json::json!(arr.value(index)))
    }
    
    // ... other types ...
}
```

## After Fix

Now queries return proper string values:

```json
{
  "host": "server01",
  "measurement": "cpu",
  "timestamp": 1766412971150078267,
  "usage": 23.5
}
```

## Testing

```bash
# 1. Write data
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01 usage=23.5'

# 2. Flush
curl -X POST http://localhost:7000/flush

# 3. Query
curl -X POST http://localhost:7000/query/mydb \
  -d "SELECT * FROM mydb_cpu"
```

**Expected output:**
```json
[
  {
    "timestamp": 1766412971150078267,
    "measurement": "cpu",
    "host": "server01",
    "usage": 23.5
  }
]
```

## Supported Data Types

After this fix, the query engine supports:

✅ **Integers:**
- `Int64`

✅ **Floats:**
- `Float64`

✅ **Strings:**
- `Utf8` (traditional)
- `Utf8View` (new, efficient)
- `LargeUtf8` (for large strings)

✅ **Boolean:**
- `Boolean`

✅ **Timestamps:**
- `Timestamp(Nanosecond, None)`

## Why This Happened

**Version Compatibility:**
- Arrow/DataFusion updates introduced `Utf8View`
- Default string type changed from `Utf8` to `Utf8View`
- Our code needed to support both old and new types

## Future Proofing

The fix handles all common string types, so future Arrow updates should work seamlessly.

## Related Files

- `src/query_engine.rs` - JSON conversion logic (lines ~90-135)

## Performance Impact

**None!** The fix only affects type conversion, not query performance. `Utf8View` is actually more efficient than traditional `Utf8`.

## Summary

Added support for `Utf8View` and `LargeUtf8` string types in the query engine's JSON conversion, fixing the "Unsupported type" error for string columns.