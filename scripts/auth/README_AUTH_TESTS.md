# Authentication Test Scripts

This directory contains test scripts for validating HTTP Basic Authentication functionality.

## Test Scripts

### 1. `test_auth.sh` - Quick Authentication Test

**Purpose:** Quick validation of authentication endpoints with current configuration.

**Usage:**
```bash
# Start the service first
cargo run --release

# In another terminal, run the test
cd scripts
./test_auth.sh
```

**What it tests:**
- ✅ Public endpoints (always accessible): `/health`, `/stats`
- ✅ Protected endpoints with no auth (works when auth disabled)
- ✅ Protected endpoints with correct credentials
- ✅ Protected endpoints with wrong credentials (should fail)
- ✅ Write operations
- ✅ Query operations
- ✅ Management operations (flush, compact, etc.)

**Expected Results:**

| Scenario | Auth Disabled | Auth Enabled |
|----------|---------------|--------------|
| Public endpoints (no auth) | ✅ 200 | ✅ 200 |
| Public endpoints (wrong auth) | ✅ 200 | ✅ 200 |
| Protected endpoints (no auth) | ✅ 200 | ❌ 401 |
| Protected endpoints (correct auth) | ✅ 200 | ✅ 200 |
| Protected endpoints (wrong auth) | ✅ 200 (ignored) | ❌ 401 |

### 2. `test_auth_comprehensive.sh` - Full Authentication Test Suite

**Purpose:** Comprehensive testing that validates both auth-enabled and auth-disabled scenarios.

**Usage:**
```bash
# Run from project root
./scripts/test_auth_comprehensive.sh

# The script will:
# 1. Backup your config.toml
# 2. Test with auth disabled (requires service restart)
# 3. Test with auth enabled (requires service restart)
# 4. Restore original config
```

**What it tests:**

**Phase 1: Auth Disabled**
- Public endpoints work without credentials
- Protected endpoints work without credentials
- Protected endpoints work with credentials (ignored)

**Phase 2: Auth Enabled**
- Public endpoints still work without credentials
- Public endpoints work even with wrong credentials
- Protected endpoints reject requests without credentials (401)
- Protected endpoints reject requests with wrong credentials (401)
- Protected endpoints accept requests with correct credentials (200)

**Phase 3: Advanced Cases**
- Query via GET and POST methods
- Compact operations
- Schema operations
- Edge cases and malformed auth headers

## Configuration

Authentication is configured in `config.toml`:

```toml
[auth]
# Enable HTTP Basic Authentication
enabled = false  # Change to true to enable

# Credentials
username = "admin"
password = "admin2026"

# Public endpoints (always accessible)
public_endpoints = ["/health", "/stats"]
```

## Authentication Endpoints

### Always Public (No Auth Required)
- `GET /health` - Health check
- `GET /stats` - System statistics

### Protected (Auth Required when enabled)
- `POST /write/:db` - Write data
- `GET /query/:db` - Query data
- `POST /query/:db` - Query data (POST method)
- `GET /tables/:db` - List tables
- `GET /schema/:db/:table` - Get table schema
- `POST /flush` - Flush cache
- `POST /compact/:db` - Trigger compaction
- `GET /partitions/:catalog/:schema/:table` - List partitions
- `DELETE /partitions/:catalog/:schema/:table/:date` - Delete partition
- `POST /retention/:catalog/:schema/:table` - Apply retention

## Security Best Practices

### 1. Enable Authentication in Production

```toml
[auth]
enabled = true
username = "admin"
password = "your-strong-password-here"
```

### 2. Protect Config File

```bash
chmod 600 config.toml
```

### 3. Use Strong Passwords

```bash
# Generate a strong password
openssl rand -base64 32
```

### 4. Use HTTPS in Production

The service supports HTTP only. In production:
- Deploy behind a reverse proxy (nginx, traefik, etc.)
- Enable HTTPS/TLS at the proxy level
- Use the proxy for additional security (rate limiting, IP filtering, etc.)

### 5. Network Isolation

- Bind to `127.0.0.1` for localhost-only access
- Use firewall rules to restrict access
- Deploy in a private network/VPC

## Example: Testing Manually with curl

### Without Authentication

```bash
# Public endpoint (always works)
curl http://localhost:7000/health

# Protected endpoint (works only if auth disabled)
curl http://localhost:7000/tables/mydb
```

### With Authentication

```bash
# Correct credentials
curl -u admin:admin2026 http://localhost:7000/tables/mydb

# Wrong credentials (will fail with 401)
curl -u admin:wrongpass http://localhost:7000/tables/mydb
```

### Write Data with Authentication

```bash
# Write line protocol data
curl -u admin:admin2026 \
  -X POST \
  -d 'cpu,host=server01 value=23.5' \
  http://localhost:7000/write/mydb
```

### Query Data with Authentication

```bash
# Query via GET
curl -u admin:admin2026 \
  'http://localhost:7000/query/mydb?q=SELECT * FROM cpu LIMIT 10'

# Query via POST
curl -u admin:admin2026 \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cpu LIMIT 10"}' \
  http://localhost:7000/query/mydb
```

## Troubleshooting

### Test Failures

**Problem:** Tests fail with "Service not available"
- **Solution:** Ensure the service is running: `cargo run --release`

**Problem:** Tests fail with wrong status codes
- **Solution:** Check `config.toml` auth settings match test expectations

**Problem:** "401 Unauthorized" when auth should be disabled
- **Solution:** Verify `auth.enabled = false` in config and restart service

### Authentication Not Working

**Problem:** Protected endpoints accessible without auth when it's enabled
- **Solution:**
  1. Verify `auth.enabled = true` in config.toml
  2. Restart the service
  3. Check logs for authentication initialization messages

**Problem:** Correct credentials rejected
- **Solution:**
  1. Verify username/password in config.toml
  2. Check for extra spaces or special characters
  3. Try URL-encoding special characters in password

## Test Output Example

```
==========================================
Phase 2: Testing with Authentication ENABLED
==========================================
INFO: Setting auth.enabled = true in config...

INFO: Testing public endpoints (should still work without auth)...
✓ PASS: Health endpoint (no auth, auth enabled) (expected 200, got 200)
✓ PASS: Stats endpoint (no auth, auth enabled) (expected 200, got 200)

INFO: Testing protected endpoints WITHOUT auth (should be rejected)...
✓ PASS: Write endpoint (no auth, auth enabled) (expected 401, got 401)
✓ PASS: Query endpoint (no auth, auth enabled) (expected 401, got 401)

INFO: Testing protected endpoints WITH CORRECT auth (should work)...
✓ PASS: Write endpoint (correct auth) (expected 200, got 200)
✓ PASS: Query endpoint (correct auth) (expected 200, got 200)

==========================================
Test Summary
==========================================
Passed: 24
Failed: 0
Total:  24

✓ All tests passed!
```

## Integration with CI/CD

Add to your CI pipeline:

```yaml
# Example GitHub Actions
- name: Test Authentication
  run: |
    cargo run --release &
    sleep 5
    ./scripts/test_auth.sh
    kill %1
```

## Related Documentation

- Main README: `../README.md`
- API Documentation: Check HTTP endpoints in `src/main.rs`
- Configuration: `../config.toml`
