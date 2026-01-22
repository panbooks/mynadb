# Custom TableProvider Architecture

This document describes the custom `ParquetTableProvider` implementation that can optionally replace DataFusion's built-in `ListingTable` and `register_parquet()` approaches for table registration and query execution.

## Configuration

The table provider implementation is configurable via `config.toml`:

```toml
[query]
# Use custom ParquetTableProvider instead of DataFusion's ListingTable
# When false (default): Uses DataFusion's built-in ListingTable and register_parquet
# When true: Uses custom ParquetTableProvider with manual Parquet reading
use_custom_table_provider = false
```

| Value | Table Provider | Description |
|-------|---------------|-------------|
| `false` (default) | ListingTable | DataFusion's built-in approach with automatic optimizations |
| `true` | ParquetTableProvider | Custom implementation with manual control |

## Overview

The custom TableProvider provides full control over the query execution path, enabling:
- Direct Parquet file reading with custom logic
- Partition pruning at the provider level
- Integration with the existing metadata cache
- Future extensibility for custom optimizations

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Query Flow                                      │
└─────────────────────────────────────────────────────────────────────────────┘

  SQL Query: "SELECT * FROM mydb.service_1.cpu WHERE date >= '2024-01-01'"
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           QueryEngine                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  execute_query_with_db()                                             │   │
│  │    1. Extract table references from SQL                              │   │
│  │    2. Call register_table() for each table                           │   │
│  │    3. Execute via ctx.sql()                                          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Table Registration                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  register_table()                                                    │   │
│  │    ├── Check metadata cache for partition info                       │   │
│  │    ├── Discover partitions if cache miss                             │   │
│  │    └── Call register_partitioned_table() or register_simple_table()  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    │                                        │
│                    ┌───────────────┴───────────────┐                       │
│                    │  use_custom_table_provider?   │                       │
│                    └───────────────┬───────────────┘                       │
│                 false              │              true                      │
│           ┌────────────────────────┴────────────────────────┐              │
│           ▼                                                  ▼              │
│  ┌─────────────────────┐                      ┌─────────────────────────┐  │
│  │ _listing() methods  │                      │ _custom() methods       │  │
│  │                     │                      │                         │  │
│  │ Uses DataFusion's   │                      │ Uses custom             │  │
│  │ ListingTable /      │                      │ ParquetTableProvider    │  │
│  │ register_parquet    │                      │                         │  │
│  │ (partitioned=false) │                      │ (partitioned=true)      │  │
│  └─────────────────────┘                      └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       ParquetTableProvider                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Implements: TableProvider trait                                     │   │
│  │                                                                      │   │
│  │  Fields:                                                             │   │
│  │    - schema: SchemaRef (includes partition column if partitioned)    │   │
│  │    - files: RwLock<Vec<ParquetFileInfo>>                            │   │
│  │    - config: ParquetTableConfig                                      │   │
│  │    - base_path: PathBuf                                              │   │
│  │                                                                      │   │
│  │  Key Methods:                                                        │   │
│  │    - try_new(): Discover files and infer schema                      │   │
│  │    - scan(): Create execution plan with filters/projection           │   │
│  │    - supports_filters_pushdown(): Report filter capabilities         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ParquetTableProvider::scan()                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  1. Compute projected schema from projection indices                 │   │
│  │  2. Get all files from internal file list                            │   │
│  │  3. If partitioned: filter files by partition predicates             │   │
│  │  4. Create ParquetExecPlan with filtered files                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          ParquetExecPlan                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Implements: ExecutionPlan trait                                     │   │
│  │                                                                      │   │
│  │  Fields:                                                             │   │
│  │    - schema: SchemaRef (projected)                                   │   │
│  │    - files: Vec<ParquetFileInfo> (after partition pruning)           │   │
│  │    - projection: Option<Vec<usize>>                                  │   │
│  │    - filters: Vec<Expr>                                              │   │
│  │    - limit: Option<usize>                                            │   │
│  │    - config: ParquetTableConfig                                      │   │
│  │                                                                      │   │
│  │  Key Methods:                                                        │   │
│  │    - execute(): Return stream of RecordBatches                       │   │
│  │    - properties(): Return plan properties (partitioning, etc.)       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      ParquetExecPlan::execute()                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  For each file (up to 4 concurrent):                                 │   │
│  │    1. Open Parquet file                                              │   │
│  │    2. Build projection mask from target schema                       │   │
│  │    3. Read batches with ParquetRecordBatchReaderBuilder              │   │
│  │    4. Add partition column if partitioned table                      │   │
│  │    5. Yield RecordBatch to stream                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                           RecordBatch Stream
                                    │
                                    ▼
                          Query Results to Client
```

## Component Details

### 1. ParquetTableConfig

Configuration struct for the table provider.

```rust
pub struct ParquetTableConfig {
    pub filter_pushdown: bool,      // Enable filter pushdown (default: true)
    pub projection_pushdown: bool,  // Enable projection pushdown (default: true)
    pub table_name: String,         // Table name for display/logging
    pub batch_size: usize,          // Batch size for reading (default: 8192)
    pub partitioned: bool,          // Is this a partitioned table?
    pub partition_column: String,   // Partition column name (default: "date")
}
```

### 2. ParquetFileInfo

Metadata about a single Parquet file.

```rust
pub struct ParquetFileInfo {
    pub path: PathBuf,                           // Full path to file
    pub partition_values: HashMap<String, String>, // e.g., {"date": "2024-01-15"}
    pub file_size: u64,                          // File size in bytes
}
```

### 3. ParquetTableProvider

The core TableProvider implementation.

**Responsibilities:**
- Discover Parquet files in table directory
- Infer schema from first Parquet file
- Handle Hive-style partitions (date=YYYY-MM-DD)
- Create execution plans for queries

**File Discovery:**
```
data/mydb/service_1/cpu/
├── date=2024-01-01/
│   ├── L0_001.parquet
│   └── L1_001.parquet
├── date=2024-01-02/
│   └── L0_001.parquet
└── date=2024-01-03/
    └── L0_001.parquet
```

For partitioned tables, the provider:
1. Scans for `date=YYYY-MM-DD` directories
2. Collects all `.parquet` files in each partition
3. Stores partition value in `ParquetFileInfo.partition_values`

### 4. ParquetExecPlan

Custom ExecutionPlan for Parquet file reading.

**Key Features:**
- Parallel file reading (up to 4 concurrent)
- Projection pushdown to Parquet reader
- Partition column injection for partitioned tables
- Streaming results via `SendableRecordBatchStream`

## Partition Pruning

The provider extracts partition filters from query predicates:

```sql
SELECT * FROM cpu WHERE date >= '2024-01-15' AND date < '2024-01-20'
```

**Filter Extraction:**
```rust
enum PartitionFilter {
    Eq(String),    // date = '2024-01-15'
    Gt(String),    // date > '2024-01-15'
    GtEq(String),  // date >= '2024-01-15'
    Lt(String),    // date < '2024-01-20'
    LtEq(String),  // date <= '2024-01-20'
}
```

**Pruning Process:**
1. Extract partition filters from `Expr` tree
2. For each file, check if partition value matches all filters
3. Only include files that pass all partition filters

**Example:**
```
Query: WHERE date >= '2024-01-15' AND date < '2024-01-20'

Files before pruning: 30 files (30 days of data)
Files after pruning:  5 files (only 2024-01-15 to 2024-01-19)
```

## Projection Pushdown

Column selection is pushed to the Parquet reader level:

```sql
SELECT id, timestamp FROM cpu WHERE ...
```

**Process:**
1. DataFusion passes projection indices to `scan()`
2. Provider computes `projected_schema` with only requested columns
3. `ParquetExecPlan` maps column names to file column indices
4. Parquet reader only reads requested columns from file

**Benefits:**
- Reduced I/O (only read needed columns)
- Reduced memory usage
- Faster query execution

## Data Flow

### Non-Partitioned Table Query

```
1. User Query: SELECT name FROM users WHERE id > 100

2. register_simple_table() called
   └── ParquetTableProvider::try_new("data/db/schema/users")
       └── Discovers: [users_001.parquet, users_002.parquet]
       └── Infers schema: {id: Int64, name: Utf8, email: Utf8}

3. DataFusion calls scan(projection=[1], filters=[id > 100])
   └── projected_schema = {name: Utf8}
   └── Creates ParquetExecPlan with 2 files

4. execute() called
   └── For each file:
       └── Open file
       └── Create projection mask for "name" column only
       └── Read batches
       └── Reproject to target schema
       └── Yield to stream

5. Results returned to client
```

### Partitioned Table Query

```
1. User Query: SELECT * FROM cpu WHERE date = '2024-01-15'

2. register_partitioned_table() called
   └── ParquetTableProvider::try_new("data/db/schema/cpu", partitioned=true)
       └── Scans for date=* directories
       └── Discovers 30 partitions with files
       └── Schema: {timestamp: Int64, value: Float64, date: Utf8}

3. DataFusion calls scan(projection=None, filters=[date = '2024-01-15'])
   └── Extracts partition filter: Eq("2024-01-15")
   └── Filters files: 30 → 2 files (only date=2024-01-15 partition)
   └── Creates ParquetExecPlan with 2 files

4. execute() called
   └── For each file in date=2024-01-15/:
       └── Open file
       └── Read batches (all columns except date)
       └── Add "date" column with value "2024-01-15"
       └── Yield to stream

5. Results returned to client
```

## Integration Points

### With QueryEngine

```rust
// query_engine.rs

async fn register_simple_table(&self, ...) -> Result<(), String> {
    let config = ParquetTableConfig::new(format!("{}.{}.{}", catalog, schema, table))
        .with_partitioned(false);

    let provider = ParquetTableProvider::try_new(local_path, config)?;

    schema.register_table(table_name, Arc::new(provider))?;
    Ok(())
}

async fn register_partitioned_table(&self, ...) -> Result<(), String> {
    let config = ParquetTableConfig::new(format!("{}.{}.{}", catalog, schema, table))
        .with_partitioned(true);

    let provider = ParquetTableProvider::try_new(local_path, config)?;

    schema.register_table(table_name, Arc::new(provider))?;
    Ok(())
}
```

### With DataFusion

The provider implements DataFusion's `TableProvider` trait:

```rust
#[async_trait]
impl TableProvider for ParquetTableProvider {
    fn as_any(&self) -> &dyn Any;
    fn schema(&self) -> SchemaRef;
    fn table_type(&self) -> TableType;
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>>;
    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>,
                  filters: &[Expr], limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>>;
}
```

## Configuration

### Default Configuration

```rust
ParquetTableConfig {
    filter_pushdown: true,
    projection_pushdown: true,
    table_name: "parquet_table",
    batch_size: 8192,
    partitioned: false,
    partition_column: "date",
}
```

### Customization

```rust
let config = ParquetTableConfig::new("mydb.schema.table")
    .with_partitioned(true)
    .with_batch_size(4096);
```

## Performance Considerations

### Advantages

1. **Partition Pruning**: Skip entire directories of irrelevant data
2. **Projection Pushdown**: Read only needed columns from Parquet
3. **Parallel Reading**: Read up to 4 files concurrently
4. **Direct Control**: Custom logic without DataFusion overhead

### Potential Improvements

1. **Row Group Pruning**: Push filters to Parquet row group level
2. **Page-Level Filtering**: Use Parquet page index for finer pruning
3. **Bloom Filter**: Use Parquet bloom filters for string columns
4. **Async I/O**: Use tokio-based async Parquet reader
5. **Object Store**: Support S3/MinIO via object_store crate

## File Structure

```
src/query/
├── mod.rs                    # Module exports
├── table_provider.rs         # ParquetTableProvider implementation
├── executor.rs               # Custom query executor
├── analyzers/
│   └── timestamp_to_date.rs  # Timestamp → date analyzer
├── logical_optimizers/
│   └── partition_filter_pushdown.rs
└── physical_optimizers/
    └── scan_config.rs
```

## Usage Example

```rust
use crate::query::table_provider::{ParquetTableConfig, ParquetTableProvider};

// Create provider for partitioned table
let config = ParquetTableConfig::new("mydb.service_1.cpu")
    .with_partitioned(true)
    .with_batch_size(8192);

let provider = ParquetTableProvider::try_new("/data/mydb/service_1/cpu", config)?;

// Register with DataFusion
ctx.register_table("cpu", Arc::new(provider))?;

// Query with partition pruning
let df = ctx.sql("SELECT * FROM cpu WHERE date >= '2024-01-15'").await?;
let results = df.collect().await?;
```

## Comparison with Built-in Approaches

| Feature | ListingTable | register_parquet | ParquetTableProvider |
|---------|-------------|------------------|---------------------|
| Partition Pruning | Yes (via DataFusion) | No | Yes (custom) |
| Projection Pushdown | Yes | Yes | Yes |
| Filter Pushdown | Yes | Yes | Yes (partition only) |
| Custom Logic | No | No | Yes |
| Schema Caching | No | No | Possible |
| File List Control | No | No | Yes |
| Metrics | DataFusion | DataFusion | Custom |

## Future Enhancements

1. **Schema Evolution**: Handle schema changes across Parquet files
2. **Statistics Collection**: Collect min/max statistics for better pruning
3. **Caching Integration**: Integrate with existing metadata cache
4. **Object Store Support**: Add S3/MinIO support via object_store
5. **Predicate Pushdown**: Push non-partition filters to Parquet reader
