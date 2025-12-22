# Changelog

## Latest Updates

### Port Change: 3000 → 7000

All references to port 3000 have been changed to port 7000.

**Changed files:**
- `src/main.rs` - Server now binds to `0.0.0.0:7000`
- `test.sh` - Updated test script URLs
- `test_with_queries.sh` - Updated test script URLs
- `README.md` - Updated documentation
- `QUICKSTART.md` - Updated quick start guide
- `QUERY_EXAMPLES.md` - Updated example URLs
- `IMPROVEMENTS.md` - Updated example URLs
- `DATAFUSION_INTEGRATION.md` - Updated example URLs
- `CACHING_IMPROVEMENTS.md` - Updated example URLs

**How to use:**

```bash
# Server now runs on port 7000
cargo run

# Access the server
curl http://localhost:7000/health
```

### Bug Fix: String Return Type

Fixed compilation error in `write_handler`:

**Before:**
```rust
(StatusCode::OK, "Data added to cache\n")  // Error: type mismatch
```

**After:**
```rust
(StatusCode::OK, "Data added to cache\n".to_string())  // Fixed!
```

This ensures consistency with other handlers that return `String` types.

## Quick Start with New Port

```bash
# 1. Start server (port 7000)
cargo run

# 2. Create database
curl -X POST http://localhost:7000/databases \
  -H 'Content-Type: application/json' \
  -d '{"name":"mydb"}'

# 3. Write data
curl -X POST http://localhost:7000/write/mydb \
  -d 'cpu,host=server01 usage=75.5'

# 4. Query data
curl 'http://localhost:7000/query/mydb?sql=SELECT * FROM mydb_cpu'
```

## All Available Ports

The Stats TSDB uses only one port:
- **HTTP API:** `0.0.0.0:7000`

No other ports are used.