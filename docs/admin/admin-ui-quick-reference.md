# MynaDB Admin UI - Quick Reference Card

**Version**: 1.0 | **Last Updated**: 2026-02-06

---

## SQL Query Console

### Access
- **URL**: `/admin/query`
- **Sidebar**: `Query Console`

### Keyboard Shortcuts
| Action | Shortcut |
|--------|----------|
| Run Query | `Ctrl+Enter` / `Cmd+Enter` |
| Explain Query | `Ctrl+Shift+E` / `Cmd+Shift+E` |
| Comment Line | `Ctrl+/` / `Cmd+/` |
| Format Code | `Shift+Alt+F` / `Shift+Option+F` |

### Quick Actions
| Button | Function |
|--------|----------|
| **Run** | Execute SQL query |
| **Explain** | Show query plan & analysis |
| **Save** | Save query with name |
| **Templates** | Insert common SQL patterns |
| **CSV/JSON** | Export results |
| **Copy** | Copy results to clipboard |

### SQL Templates
```sql
-- Show Tables
SHOW TABLES;

-- Describe Table
DESCRIBE table_name;

-- Count Rows
SELECT COUNT(*) FROM table_name;

-- Time Bucket Aggregation
SELECT
  time_bucket('1 hour', time) as bucket,
  COUNT(*) as count,
  AVG(value) as avg_value
FROM table_name
WHERE time > now() - INTERVAL '24 hours'
GROUP BY bucket
ORDER BY bucket DESC;

-- Latest Values
SELECT *
FROM table_name
ORDER BY time DESC
LIMIT 100;
```

### Query Optimization Tips
- ✅ **Always** include `LIMIT` during development
- ✅ Use time/date filters in `WHERE` clauses
- ✅ Run `EXPLAIN` before expensive queries
- ✅ Use aggregations instead of raw data
- ❌ Avoid `SELECT *` on large tables
- ❌ Don't use `LIKE '%pattern%'` without index

### History & Saved Queries
- **History**: Last 100 queries (LocalStorage)
- **Saved**: Unlimited bookmarked queries
- **Search**: Filter by SQL or catalog name
- **Load**: Click to load into editor

---

## Storage Browser

### Access
- **URL**: `/admin/storage`
- **Sidebar**: `Storage Browser`

### Tree Structure
```
📁 Catalog (mydb)
  └─ 📂 Schema (default)
      └─ 📚 Table (sensor_data)
          └─ 📅 Partition (2024-01-10)
              ├─ 📄 L0_file_001.parquet
              ├─ 📄 L0_file_002.parquet
              └─ 📄 L1_merged.parquet
```

### Icons & Badges
| Icon/Badge | Meaning |
|------------|---------|
| 🗄️ Database | Catalog |
| 📁 Folder | Schema |
| 📚 FileStack | Table |
| 📅 Calendar | Date Partition |
| 📄 File | Parquet File |
| **L0** | Recent writes (not compacted) |
| **L1** | First-level compaction |
| **L2+** | Higher-level compaction |

### Quick Actions
| Level | Action | Purpose |
|-------|--------|---------|
| **Catalog** | Compact All | Compact entire database |
| **Schema** | Compact Schema | Compact all tables in schema |
| **Table** | Compact Table | Compact specific table |
| **Partition** | Delete | Remove old partition data |

### Compaction Levels
```
L0 ████████░░ 45%  (23 files) ← Needs compaction
L1 ███░░░░░░░ 30%  (8 files)
L2 ██░░░░░░░░ 25%  (2 files)  ← Well compacted
```

**Interpretation:**
- **High L0** → Trigger compaction
- **Balanced** → Healthy state
- **Only L2/L3** → Optimal (rare)

### Compaction Schedule (Recommended)
| Table Type | Write Volume | Frequency |
|------------|--------------|-----------|
| High-write | >1GB/day | Every 6 hours |
| Medium-write | 100MB-1GB/day | Daily |
| Low-write | <100MB/day | Weekly |

### Partition Retention
```
Hot:  0-7 days   → Keep all
Warm: 8-90 days  → Keep, compact aggressively
Cold: >90 days   → Delete or archive
```

### File Details
**Parquet Inspector shows:**
- Row count & row groups
- File size & compression (SNAPPY, GZIP, ZSTD)
- Column schema & types
- Min/Max statistics
- Null counts per column

---

## API Endpoints (Backend)

### Query Console APIs
```
POST   /admin/api/v1/query/execute
GET    /admin/api/v1/query/explain/:catalog?sql=...
GET    /admin/api/v1/query/analyze/:catalog?sql=...
GET    /admin/api/v1/query/catalogs
GET    /admin/api/v1/query/schemas/:catalog
GET    /admin/api/v1/query/columns/:catalog/:schema/:table
```

### Storage Browser APIs
```
GET    /admin/api/v1/storage/info
GET    /admin/api/v1/storage/catalogs
GET    /admin/api/v1/storage/catalogs/:catalog/schemas
GET    /admin/api/v1/storage/catalogs/:catalog/schemas/:schema/tables/:table
GET    /admin/api/v1/storage/file?path=...
DELETE /admin/api/v1/storage/catalogs/:catalog/schemas/:schema/tables/:table/partitions/:date
POST   /admin/api/v1/storage/compact/:catalog[/:schema[/:table]]
```

---

## Common Tasks

### Execute a Query
```
1. Select catalog from dropdown
2. Write SQL in editor
3. Press Ctrl+Enter or click Run
4. View results in table below
```

### Save a Query for Later
```
1. Write query in editor
2. Click Save button
3. Enter descriptive name
4. Find in Saved tab (right sidebar)
```

### Export Query Results
```
1. Run query successfully
2. Click CSV or JSON button
3. File downloads automatically
```

### View Query Execution Plan
```
1. Write query in editor
2. Click Explain button (or Ctrl+Shift+E)
3. Switch to Explain tab
4. Review partitions, files, optimizations
```

### Delete Old Partition
```
1. Navigate to table in tree
2. Find partition in Partitions table
3. Click trash icon (🗑️)
4. Confirm deletion
5. Partition and files removed
```

### Trigger Table Compaction
```
1. Navigate to table in tree
2. Click table name
3. Click "Compact Table" button
4. Wait for completion (spinner disappears)
5. Refresh tree to see new file structure
```

### Inspect Parquet File
```
1. Navigate to partition in tree
2. Expand partition to see files
3. Click on any .parquet file
4. View schema, stats, compression in right panel
```

---

## Troubleshooting

### Query Console

**Query Returns No Results**
- Verify table exists: `SHOW TABLES;`
- Check for typos in table/column names
- Add `LIMIT 10` to verify data exists

**"HTTP 500" Error**
- Check backend logs
- Verify catalog exists
- Reduce query complexity
- Check server memory usage

**History Lost**
- LocalStorage is browser-specific
- Avoid Incognito mode
- Export important queries manually

### Storage Browser

**Tree Not Loading**
- Verify backend APIs implemented
- Check data directory exists
- Refresh page (F5)

**Delete Partition Fails**
- Refresh tree first
- Check file permissions
- Verify backend logs

**File Details Not Loading**
- Check file exists on disk/S3
- Try different file in partition
- Check backend Parquet reader

---

## Performance Tips

### Query Optimization
```sql
-- ✅ Good: Use time filters and LIMIT
SELECT * FROM sensor_data
WHERE time > '2024-01-10'
  AND time < '2024-01-11'
LIMIT 1000;

-- ❌ Bad: Full table scan
SELECT * FROM sensor_data;

-- ✅ Good: Aggregation reduces data
SELECT
  time_bucket('1 hour', time) as hour,
  AVG(temperature) as avg_temp
FROM sensor_data
WHERE time > now() - INTERVAL '24 hours'
GROUP BY hour;

-- ❌ Bad: Returning millions of rows
SELECT time, temperature FROM sensor_data;
```

### Storage Management
- **Monitor L0 files**: Alert if >50 files/partition
- **Schedule compaction**: Off-peak hours
- **Delete old data**: Use retention policies
- **Use compression**: SNAPPY (fast) or ZSTD (smaller)

---

## Useful Queries

### Table Discovery
```sql
-- List all tables in a catalog
SHOW TABLES;

-- List tables across schemas
SELECT table_catalog, table_schema, table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema')
ORDER BY table_schema, table_name;
```

### Data Distribution by Day
```sql
SELECT
  DATE(time) as date,
  COUNT(*) as row_count,
  COUNT(DISTINCT sensor_id) as unique_sensors
FROM sensor_data
WHERE time > CURRENT_DATE - INTERVAL '30 days'
GROUP BY date
ORDER BY date DESC;
```

### Null Value Analysis
```sql
SELECT
  COUNT(*) as total_rows,
  COUNT(*) FILTER (WHERE temperature IS NULL) as null_temperature,
  COUNT(*) FILTER (WHERE humidity IS NULL) as null_humidity,
  ROUND(100.0 * COUNT(*) FILTER (WHERE temperature IS NULL) / COUNT(*), 2) as temp_null_pct
FROM sensor_data;
```

---

## Key Metrics to Monitor

### Query Console
- **Execution Time**: Target <1s for interactive queries
- **Row Count**: Limit to <10K for UI display
- **History Size**: Clear when >50 entries

### Storage Browser
- **L0 File Count**: Alert if >50 files/partition
- **Partition Count**: Review if >365 (1 year)
- **File Size Distribution**: Target 100-500 MB per file
- **Compaction Lag**: L0 files >24 hours old

---

## Best Practices Summary

### ✅ Do
- Use time filters in all production queries
- Run EXPLAIN before expensive operations
- Save complex queries with descriptive names
- Compact tables with high write volume daily
- Delete partitions beyond retention policy
- Monitor L0 file counts

### ❌ Don't
- Run SELECT * without LIMIT on large tables
- Delete partitions without verification
- Compact during peak hours
- Store sensitive data in query history
- Ignore compaction warnings
- Skip EXPLAIN for new queries

---

## Quick Stats Reference

### Storage Size Units
- **KB** (Kilobyte) = 1,024 bytes
- **MB** (Megabyte) = 1,024 KB
- **GB** (Gigabyte) = 1,024 MB
- **TB** (Terabyte) = 1,024 GB
- **PB** (Petabyte) = 1,024 TB

### Time Intervals (SQL)
```sql
INTERVAL '1 minute'
INTERVAL '5 minutes'
INTERVAL '1 hour'
INTERVAL '6 hours'
INTERVAL '1 day'
INTERVAL '7 days'
INTERVAL '30 days'
INTERVAL '1 year'
```

### Time Bucket Examples
```sql
time_bucket('1 minute', time)   -- Minute-level aggregation
time_bucket('5 minutes', time)  -- 5-minute buckets
time_bucket('1 hour', time)     -- Hourly aggregation
time_bucket('1 day', time)      -- Daily aggregation
```

---

## Support

For detailed documentation, see: `/docs/admin-ui-query-storage-guide.md`

**Common Links:**
- Full User Guide: `admin-ui-query-storage-guide.md`
- Architecture: `../CLAUDE.md`
- API Reference: See Backend API section above

---

*Quick Reference v1.0 - 2026-02-06*
