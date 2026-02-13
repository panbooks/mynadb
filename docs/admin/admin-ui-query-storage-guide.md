# MynaDB Admin UI - Query Console & Storage Browser Guide

**Version**: 1.0
**Last Updated**: 2026-02-06

This guide covers two powerful new features added to the MynaDB Admin Dashboard:
- **SQL Query Console**: Interactive SQL editor with query execution and analysis
- **Storage Browser**: File system explorer for Parquet data with compaction management

---

## Table of Contents

1. [Overview](#overview)
2. [SQL Query Console](#sql-query-console)
   - [Getting Started](#getting-started-query-console)
   - [Writing and Running Queries](#writing-and-running-queries)
   - [Query Results](#query-results)
   - [Query Explain & Analysis](#query-explain--analysis)
   - [Query History](#query-history)
   - [Saved Queries](#saved-queries)
   - [Keyboard Shortcuts](#keyboard-shortcuts)
3. [Storage Browser](#storage-browser)
   - [Getting Started](#getting-started-storage-browser)
   - [Navigating the Storage Tree](#navigating-the-storage-tree)
   - [Viewing Storage Details](#viewing-storage-details)
   - [Managing Partitions](#managing-partitions)
   - [Compaction Management](#compaction-management)
   - [Parquet File Inspector](#parquet-file-inspector)
4. [Best Practices](#best-practices)
5. [Troubleshooting](#troubleshooting)
6. [Backend API Reference](#backend-api-reference)

---

## Overview

### SQL Query Console

The SQL Query Console provides a powerful, web-based SQL editor for querying your MynaDB data. It features:

- **Monaco Editor**: Industry-standard code editor with syntax highlighting
- **Real-time Execution**: Run queries and see results instantly
- **Query Analysis**: View execution plans and optimization details
- **Export Options**: Download results as CSV or JSON
- **History Tracking**: Automatic logging of all executed queries
- **Saved Queries**: Bookmark frequently used queries

**Access**: Navigate to `Query Console` in the sidebar or visit `/admin/query`

### Storage Browser

The Storage Browser is a file system explorer that lets you inspect and manage your MynaDB storage backend. It provides:

- **Tree Navigation**: Browse catalogs, schemas, tables, and partitions
- **Storage Statistics**: View sizes, row counts, and file distributions
- **Partition Management**: Delete old partitions to free up space
- **Compaction Control**: Trigger compaction at any level
- **Parquet Inspector**: Examine file schemas and statistics

**Access**: Navigate to `Storage Browser` in the sidebar or visit `/admin/storage`

---

## SQL Query Console

### Getting Started (Query Console)

1. **Navigate to Query Console**
   - Click `Query Console` in the left sidebar
   - The page displays a SQL editor, toolbar, and results area

2. **Select a Catalog**
   - Use the `Catalog` dropdown in the toolbar
   - The first available catalog is selected automatically
   - All queries run against the selected catalog

3. **Write Your First Query**
   ```sql
   -- Show all tables in the current catalog
   SHOW TABLES;
   ```

4. **Execute the Query**
   - Click the `Run` button
   - Or press `Ctrl+Enter` (Windows/Linux) or `Cmd+Enter` (Mac)
   - Results appear in the table below

---

### Writing and Running Queries

#### SQL Editor Features

- **Syntax Highlighting**: SQL keywords, strings, and comments are color-coded
- **Auto-Indentation**: Press Enter for automatic indentation
- **Multi-line Support**: Write complex queries across multiple lines
- **Dark Theme**: Optimized for readability

#### Query Templates

Click the `Templates...` dropdown to insert common query patterns:

| Template | Description | Use Case |
|----------|-------------|----------|
| **Show Tables** | `SHOW TABLES` | List all tables in catalog |
| **Describe Table** | `DESCRIBE table_name` | View table schema |
| **Count Rows** | `SELECT COUNT(*) FROM table_name` | Get row count |
| **Time Bucket** | Aggregation with time bucketing | Time series analysis |
| **Latest Values** | `ORDER BY time DESC LIMIT 100` | Recent data |

#### Running Queries

**Method 1: Run Button**
- Click the `Run` button in the toolbar
- Query executes immediately
- Spinner shows while processing

**Method 2: Keyboard Shortcut**
- Press `Ctrl+Enter` or `Cmd+Enter`
- Faster for iterative development

**Query Execution Flow:**
1. Query is validated and sent to backend
2. Execution time is measured
3. Results are displayed in table format
4. Query is added to history (success or failure)

---

### Query Results

#### Results Table

After a successful query:
- **Column Headers**: Show column names and data types
- **Data Rows**: Display up to 1,000 rows (truncated if more)
- **Execution Time**: Badge shows query duration (e.g., "245ms")
- **Row Count**: Badge shows total rows returned

#### Exporting Results

**Export to CSV:**
```
1. Click "CSV" button in results toolbar
2. File downloads as query-results-{timestamp}.csv
3. Compatible with Excel, Google Sheets
```

**Export to JSON:**
```
1. Click "JSON" button in results toolbar
2. File downloads as query-results-{timestamp}.json
3. Pretty-printed for readability
```

**Copy to Clipboard:**
```
1. Click the copy icon (📋)
2. Results copied as JSON
3. Paste into any text editor
```

#### Handling Large Results

- Maximum 1,000 rows displayed in UI
- Use `LIMIT` clause for better performance:
  ```sql
  SELECT * FROM large_table LIMIT 100;
  ```
- Consider exporting to CSV for offline analysis
- Use aggregations to reduce result size:
  ```sql
  SELECT date, COUNT(*) FROM large_table GROUP BY date;
  ```

---

### Query Explain & Analysis

#### Understanding EXPLAIN

Click the `Explain` button (or press `Ctrl+Shift+E`) to analyze your query without executing it.

**What You Get:**

1. **Query Analysis Card**
   - **Tables**: Number of tables referenced
   - **Partitions**: Partitions to scan after pruning
   - **Files**: Parquet files that will be read
   - **Optimizations**: Number of applied optimizations

2. **Applied Optimizations**
   - Predicate Pushdown
   - Partition Pruning
   - Projection Pushdown
   - Filter Reordering
   - Bloom Filter Usage

3. **Execution Plan**
   - **Logical Plan**: Abstract query representation
   - **Physical Plan**: Actual execution strategy with operators

#### Example: Analyzing a Time Range Query

```sql
SELECT * FROM sensor_data
WHERE time >= '2024-01-10' AND time < '2024-01-11';
```

**Expected Analysis:**
- **Partitions to Scan**: 1 (only 2024-01-10)
- **Optimizations**: Partition pruning, predicate pushdown
- **Files**: 5-10 Parquet files (depending on compaction level)

**Optimization Tips:**
- ✅ Always include time/date filters for better performance
- ✅ Use indexed columns in WHERE clauses
- ✅ Avoid `SELECT *` in production queries
- ❌ Avoid full table scans on large tables

---

### Query History

#### Automatic History Tracking

Every query you run is automatically saved to local storage:
- **Success Queries**: Green checkmark icon
- **Failed Queries**: Red alert icon
- **Metadata**: Timestamp, catalog, row count, execution time

#### Viewing History

1. **History Tab** (right sidebar)
   - Shows last 100 queries (most recent first)
   - Search box filters by SQL or catalog name
   - Click any entry to load it into the editor

2. **History Entry Details**
   - **Status Icon**: Success (✓) or Error (⚠)
   - **Catalog Badge**: Which catalog was queried
   - **Timestamp**: Relative time (e.g., "5m ago")
   - **Statistics**: Row count and execution time
   - **SQL Preview**: Truncated query text

#### Managing History

**Search History:**
```
1. Click History tab
2. Type in search box
3. Results filter in real-time
```

**Load Query from History:**
```
1. Click on any history entry
2. SQL loads into editor
3. Catalog is automatically selected
4. Click Run to re-execute
```

**Clear History:**
```
1. Click "Clear History" button at bottom
2. All local history is deleted
3. Action cannot be undone
```

**Storage Details:**
- History stored in browser's LocalStorage
- Maximum 100 entries (oldest removed automatically)
- Survives page reloads
- **Not** synced across devices/browsers

---

### Saved Queries

#### Saving a Query

1. Write your query in the editor
2. Click the `Save` button
3. Enter a descriptive name (e.g., "Daily Sales Report")
4. Click `Save` in the dialog
5. Query appears in the `Saved` tab

**Example:**
```sql
-- Name: "Active Sensors Last Hour"
SELECT
  sensor_id,
  COUNT(*) as reading_count,
  AVG(temperature) as avg_temp
FROM sensor_data
WHERE time > now() - INTERVAL '1 hour'
GROUP BY sensor_id
HAVING COUNT(*) > 0
ORDER BY reading_count DESC;
```

#### Using Saved Queries

1. **Saved Tab** (right sidebar)
   - Shows all saved queries
   - Sorted by most recently updated
   - Displays query name and preview

2. **Load Saved Query**
   - Click on any saved query
   - SQL loads into editor
   - Catalog is restored
   - Ready to run

3. **Delete Saved Query**
   - Hover over saved query entry
   - Click the `×` button that appears
   - Query removed immediately

#### Organization Tips

**Naming Convention:**
- Use descriptive names: "Daily Revenue by Region"
- Include time scope: "Errors Last 24 Hours"
- Add context: "Production DB - User Signups"

**Common Use Cases:**
- Daily reporting queries
- Complex aggregations
- Troubleshooting queries
- Schema inspection queries
- Performance benchmarks

---

### Keyboard Shortcuts

| Action | Windows/Linux | Mac |
|--------|---------------|-----|
| Run Query | `Ctrl+Enter` | `Cmd+Enter` |
| Explain Query | `Ctrl+Shift+E` | `Cmd+Shift+E` |
| Format Code | `Shift+Alt+F` | `Shift+Option+F` |
| Find | `Ctrl+F` | `Cmd+F` |
| Replace | `Ctrl+H` | `Cmd+H` |
| Comment Line | `Ctrl+/` | `Cmd+/` |
| Select All | `Ctrl+A` | `Cmd+A` |
| Undo | `Ctrl+Z` | `Cmd+Z` |
| Redo | `Ctrl+Shift+Z` | `Cmd+Shift+Z` |

**Pro Tip**: Use `Ctrl+/` to quickly comment/uncomment SQL lines during development.

---

## Storage Browser

### Getting Started (Storage Browser)

1. **Navigate to Storage Browser**
   - Click `Storage Browser` in the left sidebar
   - The page shows a tree view (left) and details panel (right)

2. **Overview Panel** (top left)
   - **Backend**: Storage type (Local, S3, MinIO, OSS)
   - **Total Size**: Combined size of all data
   - **Files**: Total Parquet file count
   - **Bucket**: S3/MinIO bucket name (if applicable)

3. **Tree Navigation** (left sidebar)
   - Hierarchical view: Catalog → Schema → Table → Partition → File
   - Click chevron icons to expand/collapse
   - Click items to view details

---

### Navigating the Storage Tree

#### Tree Structure

```
📁 Catalog (e.g., "mydb")
  └─ 📂 Schema (e.g., "default")
      └─ 📚 Table (e.g., "sensor_data")
          └─ 📅 Partition (e.g., "2024-01-10")
              ├─ 📄 L0_file_001.parquet
              ├─ 📄 L0_file_002.parquet
              └─ 📄 L1_merged.parquet
```

#### Node Icons

| Icon | Type | Description |
|------|------|-------------|
| 🗄️ Database | Catalog | Top-level database |
| 📁 Folder | Schema | Logical grouping |
| 📚 FileStack | Table | Measurement/table |
| 📅 Calendar | Partition | Date partition (YYYY-MM-DD) |
| 📄 File | Parquet File | Individual data file |

#### Badges and Indicators

- **Size Badges**: Show total size (e.g., "1.2 GB")
- **Count Badges**: Show item counts (e.g., "23 tables")
- **Level Badges**: L0, L1, L2 (compaction level)
  - **L0**: Recent writes (not yet compacted)
  - **L1**: First level compaction
  - **L2+**: Higher-level compaction

---

### Viewing Storage Details

#### Catalog Details

**When You Click a Catalog:**
- **Statistics Cards**:
  - Schemas: Number of schemas
  - Tables: Total tables across all schemas
  - Partitions: Total date partitions
  - Total Size: Combined data size

- **Actions**:
  - `Compact All`: Trigger compaction for entire catalog

**Use Case**: Monitoring overall database growth and triggering full compaction.

---

#### Schema Details

**When You Click a Schema:**
- **Statistics Cards**:
  - Tables: Number of tables in schema
  - Partitions: Total partitions
  - Total Size: Schema data size

- **Actions**:
  - `Compact Schema`: Trigger compaction for all tables in schema

**Use Case**: Managing related tables together (e.g., all production tables).

---

#### Table Details

**When You Click a Table:**

**Overview Cards:**
- **Partitions**: Number of date partitions
- **Files**: Total Parquet files
- **Total Rows**: Combined row count
- **Total Size**: Table data size

**Compaction Level Distribution:**
- Visual breakdown of files by level
- Progress bars show size distribution
- Helps identify tables needing compaction

**Example:**
```
L0 ████████░░ 45%  (23 files, 450 MB)  ← Many small files
L1 ███░░░░░░░ 30%  (8 files, 300 MB)
L2 ██░░░░░░░░ 25%  (2 files, 250 MB)   ← Fewer, larger files
```

**Interpretation:**
- High L0 percentage = Needs compaction
- Balanced distribution = Well-compacted
- Only L2/L3 = Fully compacted (ideal)

**Partitions Table:**
| Date | Files | Rows | Size | Actions |
|------|-------|------|------|---------|
| 2024-01-10 | 15 | 1.2M | 245 MB | 🗑️ Delete |
| 2024-01-09 | 8 | 980K | 198 MB | 🗑️ Delete |

**Actions:**
- `Compact Table`: Merge files at this table level
- `Delete` (per partition): Remove old partition data

---

### Managing Partitions

#### Understanding Partitions

MynaDB stores data in **date-based partitions**:
- Format: `date=YYYY-MM-DD/`
- One directory per day
- Enables fast time-range queries
- Simplifies data retention

#### Viewing Partition Details

Click a partition in the tree to see:
- **Date**: Partition date
- **Files**: List of Parquet files
- **Size**: Partition storage size
- **Rows**: Total row count

#### Deleting Partitions

**When to Delete:**
- Data older than retention policy (e.g., 90 days)
- Test data no longer needed
- Corrupted partitions

**How to Delete:**
1. Navigate to the table
2. Find partition in the Partitions table
3. Click the 🗑️ (trash) icon
4. Confirm in the dialog
5. Partition and all files are removed

**Deletion Dialog:**
```
┌─────────────────────────────────────┐
│ Delete Partition                     │
├─────────────────────────────────────┤
│ Are you sure you want to delete     │
│ the partition 2024-01-10?           │
│                                      │
│ This action cannot be undone.       │
├─────────────────────────────────────┤
│          [Cancel]  [Delete]         │
└─────────────────────────────────────┘
```

**Result:**
- Files deleted from storage
- Freed bytes reported
- Table statistics updated
- Tree view refreshed

**⚠️ Warning**: Deletion is permanent. Always verify the date before confirming.

---

### Compaction Management

#### What is Compaction?

**Compaction** merges many small Parquet files into fewer, larger files:
- **Benefits**:
  - Faster query performance
  - Reduced file system overhead
  - Better compression ratios
  - Lower memory usage
- **Levels**:
  - **L0**: Newly written files (not compacted)
  - **L1**: First-level merge (5-10 L0 files → 1 L1 file)
  - **L2+**: Higher-level merges (multiple L1 → L2, etc.)

#### When to Compact

**Indicators You Need Compaction:**
- ✅ High L0 file count (>20 files per partition)
- ✅ Many small files (<10 MB each)
- ✅ Slow query performance
- ✅ High L0 percentage in distribution chart

**Optimal Compaction Strategy:**
- Schedule compaction during off-peak hours
- Compact tables with high write volume daily
- Use full compaction weekly for critical tables

#### Triggering Compaction

**Catalog-Level Compaction:**
```
1. Click on a catalog in the tree
2. Click "Compact All" button
3. All tables in catalog are compacted
```

**Schema-Level Compaction:**
```
1. Click on a schema in the tree
2. Click "Compact Schema" button
3. All tables in schema are compacted
```

**Table-Level Compaction:**
```
1. Click on a table in the tree
2. Click "Compact Table" button
3. Only this table is compacted
```

**Compaction Process:**
1. Button shows spinner: "Compacting..."
2. Backend merges L0 files into L1
3. Old files deleted, new files created
4. Tree view updates automatically

**Duration:**
- Small tables (<1 GB): Seconds
- Medium tables (1-10 GB): Minutes
- Large tables (>10 GB): 10-30 minutes

---

### Parquet File Inspector

#### Opening a File

1. Navigate to a table in the tree
2. Expand a partition
3. Click on any Parquet file
4. File details appear in the right panel

#### File Overview Cards

**Statistics:**
- **Row Count**: Total rows in file
- **Row Groups**: Number of Parquet row groups
- **File Size**: Storage size in bytes
- **Compression**: Compression codec (SNAPPY, GZIP, ZSTD, etc.)

**Example:**
```
┌─────────────┬──────────────┬────────────┬──────────────┐
│ Row Count   │ Row Groups   │ File Size  │ Compression  │
├─────────────┼──────────────┼────────────┼──────────────┤
│   1,245,892 │           12 │   245 MB   │    SNAPPY    │
└─────────────┴──────────────┴────────────┴──────────────┘
```

#### Schema Table

Shows all columns in the Parquet file:

| Column | Type | Nullable | Min | Max | Nulls |
|--------|------|----------|-----|-----|-------|
| time | Timestamp | No | 2024-01-10 00:00:00 | 2024-01-10 23:59:59 | 0 |
| sensor_id | String | No | "sensor_001" | "sensor_999" | 0 |
| temperature | Float64 | Yes | -10.5 | 45.3 | 123 |
| humidity | Float64 | Yes | 20.0 | 95.5 | 45 |

**Column Information:**
- **Type**: Parquet data type (Int64, String, Timestamp, etc.)
- **Nullable**: Whether column allows NULL values
- **Min/Max**: Range of values (from Parquet statistics)
- **Nulls**: Count of NULL values

**Use Cases:**
- **Schema Validation**: Verify column types are correct
- **Data Quality**: Check for unexpected NULL values
- **Performance**: Use min/max for partition pruning validation
- **Debugging**: Identify type mismatches or corrupted data

---

## Best Practices

### SQL Query Console

#### Query Performance

**✅ Do:**
- Always use `LIMIT` during development
- Include time/date filters in WHERE clauses
- Use `EXPLAIN` before running expensive queries
- Index frequently queried columns
- Use aggregations instead of returning raw data

**❌ Don't:**
- Run `SELECT * FROM large_table` without LIMIT
- Use `LIKE '%pattern%'` on unindexed columns
- Join large tables without filters
- Run DDL statements without backups

#### Query Organization

**Save Common Queries:**
```sql
-- Name: "Hourly Error Count"
SELECT
  time_bucket('1 hour', time) as hour,
  COUNT(*) as error_count
FROM logs
WHERE level = 'ERROR' AND time > now() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;
```

**Use Descriptive Names:**
- ✅ "Production CPU Usage - Last 7 Days"
- ❌ "query1", "test", "temp"

**Document Complex Queries:**
```sql
-- Purpose: Calculate daily active users with session duration
-- Schedule: Run daily at 6 AM
-- Owner: Analytics Team
SELECT
  date_trunc('day', session_start) as day,
  COUNT(DISTINCT user_id) as dau,
  AVG(session_duration_sec) as avg_session
FROM user_sessions
WHERE session_start >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY day
ORDER BY day DESC;
```

### Storage Browser

#### Partition Management

**Retention Strategy:**
```
Hot Data:   0-7 days    → Keep all partitions, no deletion
Warm Data:  8-90 days   → Keep, but compact aggressively
Cold Data:  >90 days    → Delete or archive to S3 Glacier
```

**Automated Retention:**
- Use MynaDB's built-in retention policies (see CLAUDE.md)
- Manual deletion via UI for ad-hoc cleanup
- Always verify date before deleting

#### Compaction Schedule

**Recommended Schedule:**

| Table Type | Write Volume | Compaction Frequency |
|------------|--------------|---------------------|
| High-write | >1GB/day | Every 6 hours |
| Medium-write | 100MB-1GB/day | Daily |
| Low-write | <100MB/day | Weekly |
| Archive | Rare | Monthly |

**Monitoring:**
- Check L0 file count daily
- Alert if L0 count > 50 files
- Monitor compaction completion time

#### Storage Optimization

**Identify Space Hogs:**
1. Browse catalogs and sort by size
2. Check tables with >100 partitions
3. Look for L0-heavy tables (not compacted)
4. Delete old test data

**Free Up Space:**
1. Delete old partitions (beyond retention)
2. Compact tables (reduces file count, not size)
3. Enable compression (SNAPPY or ZSTD)
4. Archive cold data to cheaper storage

---

## Troubleshooting

### SQL Query Console

#### Query Fails with "HTTP 500"

**Symptoms:**
- Query execution returns error
- Message: "HTTP 500: Internal Server Error"

**Causes:**
- Backend API not running
- Database connection lost
- Query timeout
- Out of memory

**Solutions:**
1. Check backend logs for errors
2. Verify catalog exists: `SHOW TABLES;`
3. Reduce query complexity or add LIMIT
4. Check server memory usage

---

#### Results Not Showing

**Symptoms:**
- Click Run, but no results appear
- No error message displayed

**Causes:**
- Empty result set
- Frontend-backend connection issue
- Query still running (large dataset)

**Solutions:**
1. Check if query returned 0 rows
2. Add `LIMIT 10` to verify data exists
3. Check browser console (F12) for errors
4. Wait longer for large queries (watch spinner)

---

#### History/Saved Queries Missing

**Symptoms:**
- Query history is empty
- Saved queries disappeared

**Causes:**
- Browser cleared LocalStorage
- Different browser/device
- Private/Incognito mode

**Solutions:**
1. LocalStorage is browser-specific (not synced)
2. Avoid Incognito mode for persistent storage
3. Export important queries to file:
   ```javascript
   // Browser console
   localStorage.getItem('mynadb-saved-queries')
   ```

---

### Storage Browser

#### Tree Not Loading

**Symptoms:**
- Tree shows "No catalogs found"
- Spinner never stops

**Causes:**
- Backend storage API not implemented
- No data in data directory
- Wrong catalog path in config

**Solutions:**
1. Verify backend API endpoints (see API Reference)
2. Check `data_dir` in config.toml
3. Write test data first:
   ```bash
   curl -X POST http://localhost:7000/write/testdb \
     -d 'temperature,sensor=001 value=23.5 1704067200000000000'
   ```

---

#### Partition Delete Fails

**Symptoms:**
- Click delete, but partition remains
- Error: "Partition not found"

**Causes:**
- Files already deleted by retention job
- Permission denied on file system
- S3 bucket policy restricts deletion

**Solutions:**
1. Refresh tree (click refresh icon)
2. Check backend logs for permission errors
3. Verify user has write access to S3/local filesystem
4. Check if partition was already deleted

---

#### File Details Not Showing

**Symptoms:**
- Click on Parquet file, but details don't load
- Shows "Loading..." indefinitely

**Causes:**
- Backend cannot read Parquet file
- File path encoding issue
- Corrupted file

**Solutions:**
1. Verify file exists on disk/S3
2. Check file permissions
3. Try a different file in same partition
4. Check backend logs for Parquet read errors

---

## Backend API Reference

### Required API Endpoints

The following endpoints must be implemented in the backend to support the Query Console and Storage Browser features.

---

### Query Console APIs

#### Execute Query

```
POST /admin/api/v1/query/execute
```

**Request Body:**
```json
{
  "sql": "SELECT * FROM sensor_data LIMIT 100",
  "catalog": "mydb",
  "format": "json",
  "limit": 1000
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "columns": [
      { "name": "time", "data_type": "Timestamp", "nullable": false },
      { "name": "sensor_id", "data_type": "String", "nullable": false },
      { "name": "value", "data_type": "Float64", "nullable": true }
    ],
    "rows": [
      { "time": "2024-01-10T12:00:00Z", "sensor_id": "sensor_001", "value": 23.5 },
      { "time": "2024-01-10T12:01:00Z", "sensor_id": "sensor_002", "value": 24.1 }
    ],
    "row_count": 2,
    "execution_time_ms": 245,
    "truncated": false
  }
}
```

---

#### Explain Query

```
GET /admin/api/v1/query/explain/:catalog?sql=SELECT+*+FROM+table
```

**Response:**
```json
{
  "success": true,
  "data": {
    "logical_plan": "Projection: time, sensor_id, value\n  Filter: time >= '2024-01-10'\n    TableScan: sensor_data",
    "physical_plan": "ProjectionExec\n  CoalesceBatchesExec\n    FilterExec\n      ParquetExec: partitions=[5], predicate=time >= '2024-01-10'",
    "optimizations": ["partition_pruning", "predicate_pushdown", "projection_pushdown"]
  }
}
```

---

#### Analyze Query

```
GET /admin/api/v1/query/analyze/:catalog?sql=SELECT+*+FROM+table
```

**Response:**
```json
{
  "success": true,
  "data": {
    "tables": ["sensor_data"],
    "time_range": { "start": "2024-01-10", "end": "2024-01-11" },
    "partitions_to_scan": 1,
    "files_to_scan": 8,
    "optimizations": ["partition_pruning", "predicate_pushdown"],
    "plan_summary": "Scanning 1 partition (2024-01-10) with 8 files, ~1.2M rows"
  }
}
```

---

#### List Catalogs

```
GET /admin/api/v1/query/catalogs
```

**Response:**
```json
{
  "success": true,
  "data": ["mydb", "testdb", "production"]
}
```

---

#### List Schemas

```
GET /admin/api/v1/query/schemas/:catalog
```

**Response:**
```json
{
  "success": true,
  "data": ["default", "service_1", "service_2"]
}
```

---

#### Get Table Columns

```
GET /admin/api/v1/query/columns/:catalog/:schema/:table
```

**Response:**
```json
{
  "success": true,
  "data": [
    { "name": "time", "data_type": "Timestamp" },
    { "name": "sensor_id", "data_type": "String" },
    { "name": "temperature", "data_type": "Float64" },
    { "name": "humidity", "data_type": "Float64" }
  ]
}
```

---

### Storage Browser APIs

#### Storage Info

```
GET /admin/api/v1/storage/info
```

**Response:**
```json
{
  "success": true,
  "data": {
    "backend": "S3",
    "bucket": "mynadb-data",
    "endpoint": "https://s3.amazonaws.com",
    "total_size_bytes": 52428800000,
    "file_count": 1245
  }
}
```

---

#### List Catalogs

```
GET /admin/api/v1/storage/catalogs
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "name": "mydb",
      "schemas": 3,
      "tables": 15,
      "partitions": 450,
      "total_size_bytes": 10485760000
    },
    {
      "name": "testdb",
      "schemas": 1,
      "tables": 5,
      "partitions": 30,
      "total_size_bytes": 524288000
    }
  ]
}
```

---

#### List Schemas

```
GET /admin/api/v1/storage/catalogs/:catalog/schemas
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "catalog": "mydb",
      "name": "default",
      "tables": 8,
      "partitions": 240,
      "total_size_bytes": 5242880000
    },
    {
      "catalog": "mydb",
      "name": "service_1",
      "tables": 7,
      "partitions": 210,
      "total_size_bytes": 5242880000
    }
  ]
}
```

---

#### Get Table Storage Info

```
GET /admin/api/v1/storage/catalogs/:catalog/schemas/:schema/tables/:table
```

**Response:**
```json
{
  "success": true,
  "data": {
    "catalog": "mydb",
    "schema": "default",
    "name": "sensor_data",
    "partitions": [
      {
        "date": "2024-01-10",
        "files": [
          {
            "path": "mydb/default/sensor_data/date=2024-01-10/L0_20240110_120000.parquet",
            "name": "L0_20240110_120000.parquet",
            "level": 0,
            "size_bytes": 10485760,
            "row_count": 125000,
            "created_at": "2024-01-10T12:30:00Z",
            "row_groups": 2,
            "compression": "SNAPPY"
          },
          {
            "path": "mydb/default/sensor_data/date=2024-01-10/L1_merged.parquet",
            "name": "L1_merged.parquet",
            "level": 1,
            "size_bytes": 52428800,
            "row_count": 625000,
            "created_at": "2024-01-10T18:00:00Z",
            "row_groups": 10,
            "compression": "SNAPPY"
          }
        ],
        "size_bytes": 62914560,
        "row_count": 750000
      }
    ],
    "total_size_bytes": 62914560,
    "total_rows": 750000,
    "file_count": 2
  }
}
```

---

#### Get Parquet File Details

```
GET /admin/api/v1/storage/file?path=mydb/default/sensor_data/date=2024-01-10/file.parquet
```

**Response:**
```json
{
  "success": true,
  "data": {
    "path": "mydb/default/sensor_data/date=2024-01-10/L0_file.parquet",
    "schema": [
      { "name": "time", "data_type": "Timestamp", "nullable": false },
      { "name": "sensor_id", "data_type": "String", "nullable": false },
      { "name": "temperature", "data_type": "Float64", "nullable": true },
      { "name": "humidity", "data_type": "Float64", "nullable": true }
    ],
    "row_count": 125000,
    "row_groups": 2,
    "file_size_bytes": 10485760,
    "compression": "SNAPPY",
    "created_at": "2024-01-10T12:30:00Z",
    "statistics": {
      "min_values": {
        "time": "2024-01-10T00:00:00Z",
        "temperature": -10.5,
        "humidity": 20.0
      },
      "max_values": {
        "time": "2024-01-10T23:59:59Z",
        "temperature": 45.3,
        "humidity": 95.5
      },
      "null_counts": {
        "temperature": 123,
        "humidity": 45
      }
    }
  }
}
```

---

#### Delete Partition

```
DELETE /admin/api/v1/storage/catalogs/:catalog/schemas/:schema/tables/:table/partitions/:date
```

**Response:**
```json
{
  "success": true,
  "data": {
    "deleted_files": 8,
    "freed_bytes": 83886080
  }
}
```

---

#### Trigger Compaction

```
POST /admin/api/v1/storage/compact/:catalog[/:schema[/:table]]
```

**Examples:**
- `POST /admin/api/v1/storage/compact/mydb` - Compact entire catalog
- `POST /admin/api/v1/storage/compact/mydb/default` - Compact schema
- `POST /admin/api/v1/storage/compact/mydb/default/sensor_data` - Compact table

**Response:**
```json
{
  "success": true,
  "data": {
    "message": "Compaction triggered for mydb.default.sensor_data",
    "files_merged": 23,
    "new_files_created": 5
  }
}
```

---

### Error Responses

All endpoints should return errors in this format:

```json
{
  "success": false,
  "error": "Table not found: sensor_data",
  "message": "The specified table does not exist in catalog 'mydb'"
}
```

**Common HTTP Status Codes:**
- `200 OK` - Success
- `400 Bad Request` - Invalid SQL or parameters
- `404 Not Found` - Catalog/table/file not found
- `500 Internal Server Error` - Query execution failed, database error
- `503 Service Unavailable` - Backend overloaded

---

## Appendix

### SQL Query Examples

#### Time Series Aggregation
```sql
SELECT
  time_bucket('5 minutes', time) as bucket,
  sensor_id,
  AVG(temperature) as avg_temp,
  MIN(temperature) as min_temp,
  MAX(temperature) as max_temp,
  COUNT(*) as reading_count
FROM sensor_data
WHERE time > now() - INTERVAL '1 hour'
GROUP BY bucket, sensor_id
ORDER BY bucket DESC, sensor_id;
```

#### Latest Values Per Sensor
```sql
SELECT DISTINCT ON (sensor_id)
  sensor_id,
  time,
  temperature,
  humidity
FROM sensor_data
ORDER BY sensor_id, time DESC;
```

#### Error Rate by Hour
```sql
SELECT
  time_bucket('1 hour', time) as hour,
  COUNT(*) FILTER (WHERE level = 'ERROR') as errors,
  COUNT(*) as total,
  ROUND(100.0 * COUNT(*) FILTER (WHERE level = 'ERROR') / COUNT(*), 2) as error_rate_pct
FROM logs
WHERE time > CURRENT_DATE - INTERVAL '7 days'
GROUP BY hour
HAVING COUNT(*) FILTER (WHERE level = 'ERROR') > 0
ORDER BY hour DESC;
```

### Storage Path Patterns

**Local Storage:**
```
/data0/mynadb/data/
  └─ mydb/                         # Catalog
      └─ default/                  # Schema
          └─ sensor_data/          # Table
              └─ date=2024-01-10/  # Partition
                  ├─ L0_*.parquet
                  ├─ L1_*.parquet
                  └─ L2_*.parquet
```

**S3 Storage:**
```
s3://mynadb-bucket/
  └─ mydb/
      └─ default/
          └─ sensor_data/
              └─ date=2024-01-10/
                  └─ *.parquet
```

---

## Support & Feedback

**Documentation Issues:**
- Report inaccuracies or missing information
- Suggest improvements

**Feature Requests:**
- Query console enhancements
- Storage browser improvements
- Additional export formats

**Bug Reports:**
- UI issues
- Performance problems
- Data inconsistencies

---

**Version History:**
- v1.0 (2026-02-06): Initial release

---

*End of Guide*
