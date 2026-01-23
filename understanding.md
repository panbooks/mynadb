       Cache and Query Architecture Exploration Report

       Based on a thorough examination of the mynadb codebase, here's a comprehensive analysis of the cache and query architecture, including key integration points for adding a queryable
       Memtable.

       ---
       1. In-Memory Cache Architecture (src/storage/cache.rs)

       Current Data Storage Structure

       Sharded Cache Design:
       - 32 shards (NUM_SHARDS = 32) to reduce lock contention
       - Each shard is a Mutex<CacheData> where CacheData = HashMap<CacheKey, Vec<RecordBatch>>
       - Cache key: (catalog, schema, table, date) as a 4-tuple
       - Values: Vec<RecordBatch> - multiple batches per (catalog, schema, table, date) combination

       Data Structures:
       // Cache key (lines 50)
       type CacheKey = (String, String, String, i32);  // (catalog, schema, table, date)

       // Cache data (line 53)
       type CacheData = HashMap<CacheKey, Vec<RecordBatch>>;

       // Shard structure (lines 62-100)
       struct CacheShard {
           data: Mutex<CacheData>,
           size_bytes: AtomicUsize,
       }

       How Data is Currently Stored

       1. Incoming Points to RecordBatch Conversion (lines 276-382):
         - DataCache::points_to_record_batch() converts parsed InfluxDB Line Protocol points to Arrow RecordBatch
         - Schema is built dynamically: timestamp + measurement + tags + fields
         - Schema is cached globally using SCHEMA_CACHE to avoid rebuilding for identical tag/field sets
         - Field types are inferred: Float64, Int64, String, Boolean (nullable except timestamp)
       2. Schema Structure (inferred from points_to_record_batch):
       Schema columns in order:
       1. timestamp (Timestamp(Nanosecond, None), NOT NULL)
       2. measurement (Utf8, NOT NULL)
       3. All tag columns in sorted order (Utf8, NULLABLE)
       4. All field columns in sorted order by type (various types, NULLABLE)
       3. Shard Distribution (lines 140-147):
         - Hash-based: key.hash() % NUM_SHARDS
         - Minimal lock contention for concurrent writes

       Flush Process

       Two-Phase Flush (lines 386-481):
       - Phase 1 (Fast): Swap data out from shards instantly using CacheShard::take()
         - Clears shard data and resets size counter
         - Minimal locking: let shard_data = shard.take()
       - Phase 2 (I/O): Write batches to disk asynchronously
         - Groups batches by (catalog, schema, table, date)
         - Calls ParquetWriter::write_batches_partitioned_sync()
         - Outputs to: data/{catalog}/{schema}/{table}/date={YYYY-MM-DD}/L0_*.parquet

       Background Flush (lines 507-541):
       - Triggers on timer (default 30s) OR size threshold (80% capacity)
       - Uses tokio::select! for dual trigger
       - Non-blocking: writers can continue while flushing

       Public API

       pub async fn add_points(
           &self,
           catalog: &str,
           schema: &str,
           table: &str,
           points: Vec<Point>,
       ) -> Result<(), CacheFullError>

       pub async fn flush(&self) -> Result<usize, String>

       pub async fn stats(&self) -> CacheStats
           pub total_rows: usize
           pub num_measurements: usize
           pub size_bytes: usize
           pub max_size_bytes: usize

       Key Finding: There is NO public method to retrieve cached data currently - only stats() returns metadata. The cache is write-only from an external perspective.

       ---
       2. Query Engine Architecture (src/query/query_engine.rs)

       Table Registration & Query Flow

       Registration Process (lines 646-743):
       1. Extract table references from SQL
       2. Check if table exists in DataFusion catalog
       3. Discover partitions (Hive-style: date=YYYY-MM-DD/)
       4. Register as ParquetTableProvider or ListingTable (configurable)
       5. Cache metadata (partition info, file listings)

       Execution Flow (lines 1253-1375):
       execute_query_with_db(sql, db_name)
         ├─ Check query cache (if enabled)
         ├─ Extract table references
         ├─ Register tables via register_table()
         ├─ Qualify unqualified table names
         ├─ Inject date predicates (partition pruning optimization)
         ├─ Transform GROUP BY time() syntax
         ├─ Execute query via execute_query(sql)
         │   └─ self.ctx.sql(sql).await?
         │   └─ df.collect().await?
         ├─ Apply fill post-processing
         ├─ Cache result
         └─ Return Vec<RecordBatch>

       Table Provider Registration

       Two Strategies (lines 746-802):

       1. Custom ParquetTableProvider (if use_custom_table_provider = true):
         - Uses custom ParquetTableProvider from src/query/table_provider.rs
         - Full control over file discovery and reading
       2. DataFusion ListingTable (default):
         - Uses built-in ListingTable with ParquetFormat
         - Partition column: "date" (Utf8)

       Both routes eventually call self.register_provider() to add to DataFusion's in-memory catalog.

       Metadata Cache

       Three-Level Caching (lines 85-264):
       1. Table Info Cache: (catalog, schema, table) → partition list
       2. File Listing Cache: directory path → file list
       3. Parquet Metadata Cache: file path → Parquet metadata

       ---
       3. Custom Table Provider (src/query/table_provider.rs)

       ParquetTableProvider Structure

       pub struct ParquetTableProvider {
           schema: SchemaRef,
           files: RwLock<Vec<ParquetFileInfo>>,
           config: ParquetTableConfig,
           base_path: PathBuf,
           object_store: Option<Arc<dyn ObjectStore>>,
       }

       Key Methods

       - try_new() - Create provider and auto-discover files/schema
       - scan() - Called by DataFusion when executing queries
         - Returns Arc<dyn ExecutionPlan>
         - Applies partition pruning based on filters
         - Calls ParquetExecPlan::new()
       - ParquetExecPlan::execute() - Streams RecordBatch
         - Reads Parquet files concurrently (up to 4 files at a time)
         - Applies projection pushdown and partition column addition
         - Returns SendableRecordBatchStream

       Important: This is a TableProvider trait implementation - DataFusion's standard interface for pluggable data sources.

       ---
       4. DataFusion Integration Points

       SessionContext & Catalog Registration

       Catalog Structure (lines 612-642):
       ctx.register_catalog(name, Arc::new(MemoryCatalogProvider))
         └─ catalog.register_schema(name, Arc::new(MemorySchemaProvider))
             └─ schema.register_table(name, Arc<dyn TableProvider>)

       Query Execution (lines 1363-1375):
       pub async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>, String> {
           let df = self.ctx.sql(sql).await?;  // Parse & plan
           let batches = df.collect().await?;  // Execute
           Ok(batches)
       }

       DataFusion Configuration

       Optimizations Enabled (lines 437-515):
       - Predicate Pushdown: pushdown_filters = true, reorder_filters = true
       - Row Group/Page Pruning: pruning = true, enable_page_index = true
       - Bloom Filters: bloom_filter_on_read = true
       - Parallel Processing: target_partitions = num_cpus
       - Batch Size: 8192 rows per batch
       - Memory Management: 16GB limit with 80% spill threshold

       ---
       5. Key Data Structures & Arrow/RecordBatch Usage

       RecordBatch Throughout the Stack

       1. Cache Input: Vec<Point> → RecordBatch (via points_to_record_batch())
       2. Cache Storage: HashMap<CacheKey, Vec<RecordBatch>>
       3. Parquet Output: Vec<RecordBatch> → ParquetWriter
       4. Query Results: Vec<RecordBatch> returned from execute_query()

       Arrow Schema Example

       Schema is built in cache.rs lines 242-274:
       Field 0: timestamp      (Timestamp(Nanosecond, None), NOT NULL)
       Field 1: measurement    (Utf8, NOT NULL)
       Field 2-N: tag_*        (Utf8, NULLABLE) - sorted by key name
       Field N+1-M: field_*    (Float64/Int64/String/Boolean, NULLABLE) - sorted by key name

       ---
       6. Integration Points for Memtable Merging

       Current Architecture Gap

       Current Flow:
       Write Points → Cache (RecordBatch shards) → Flush → Parquet Files
       Query → Parquet Files only (cache is opaque to queries)

       Missing: Queries do NOT read from the cache currently. Cached data is only visible after flush to Parquet.

       Key Integration Points for Memtable

       Option 1: Custom TableProvider for Cache (Recommended)

       Create a new MemtableProvider implementing TableProvider trait:
       - Stored in QueryEngine alongside ParquetTableProvider
       - Reads from DataCache directly
       - Returns schema from cached RecordBatches
       - Implements scan() method returning cached batches

       Location to Add:
       - New file: src/query/memtable_provider.rs
       - Modify: src/query/query_engine.rs - register MemtableProvider before ParquetTableProvider
       - Modify: src/query/table_provider.rs or create union approach

       Option 2: Union at Query Execution Level

       Merge cache batches with Parquet results after execution:
       - After execute_query() returns, union with cache data
       - Location: In execute_query_with_db() around line 1300+

       Option 3: DataFusion UnionExec Physical Plan

       Combine both table providers at physical plan level:
       - Create a UnionTableProvider wrapping both Memtable + Parquet
       - DataFusion will handle parallel execution

       Implementation Strategy

       Simplest Path (Option 1 - Custom TableProvider):

       1. Create MemtableProvider struct:
       pub struct MemtableProvider {
           cache: Arc<DataCache>,
           catalog: String,
           schema_name: String,
           table_name: String,
           schema: SchemaRef,
       }
       2. Implement TableProvider trait:
         - schema() - Return schema from first cached RecordBatch
         - scan() - Return MemtableExecPlan
         - supports_filters_pushdown() - Return Inexact
       3. Implement MemtableExecPlan (ExecutionPlan):
         - execute() - Collect all batches from cache for given (catalog, schema, table)
         - Apply projection and filters
         - Return as stream
       4. Integration in QueryEngine:
         - Pass cache reference to table registration
         - Check cache for data before/after registering Parquet table
         - For tables with cache data, register MemtableProvider as well
         - Use DataFusion's UNION to combine results

       Data Access Pattern

       To retrieve cached data for a table:

       Currently not possible (cache is sharded and private). Would need to add:

       impl DataCache {
           /// Get all RecordBatches for a table across all dates
           pub fn get_table_batches(
               &self,
               catalog: &str,
               schema: &str,
               table: &str,
           ) -> Vec<RecordBatch> {
               // Iterate through all shards and collect matching batches
               self.shards.iter()
                   .flat_map(|shard| {
                       let data = shard.data.lock();
                       // Get all keys matching (catalog, schema, table, *)
                   })
                   .collect()
           }
       }

       ---
       7. Query Execution Currently

       Before & After Current State

       Write Side:
       1. HTTP POST /write/{db} arrives at api.rs
       2. Points parsed by parse_line_protocol()
       3. Added to cache via cache.add_points()
       4. Background flush writes to Parquet after 30s or at 80% capacity

       Query Side:
       1. HTTP GET/POST /query/{db}?sql=... arrives at api.rs
       2. Calls query_engine.execute_query_with_db(sql, db)
       3. Tables auto-registered from Parquet files on disk
       4. Query executes against Parquet only
       5. Results returned as JSON

       Gap: Recently written data (in cache) is not visible in queries until flushed to Parquet.

       ---
       8. Summary of Files & Key Locations

       | File                         | Purpose                       | Key Components                                              |
       |------------------------------|-------------------------------|-------------------------------------------------------------|
       | src/storage/cache.rs         | In-memory buffering           | 32 sharded HashMap<CacheKey, Vec>                           |
       | src/query/query_engine.rs    | Query execution orchestration | execute_query_with_db(), table registration, metadata cache |
       | src/query/table_provider.rs  | Parquet data source           | ParquetTableProvider (TableProvider trait), ParquetExecPlan |
       | src/query/executor.rs        | Custom query pipeline         | Custom analyzer/optimizer rules                             |
       | src/services/api.rs          | HTTP handlers                 | Query routing to query_engine                               |
       | src/storage/line_protocol.rs | Point parsing                 | Point struct, field value types                             |

       ---
       9. Recommended Architecture for Memtable

       Create src/query/memtable_provider.rs:

       MemtableProvider (implements TableProvider)
         ├─ Methods:
         │  ├─ schema() - Get schema from first cached batch
         │  ├─ scan() - Return MemtableExecPlan
         │  └─ supports_filters_pushdown() - return Inexact
         │
         └─ MemtableExecPlan (implements ExecutionPlan)
            └─ execute() - Collect RecordBatches from cache and stream

       Integration Points:
       1. In QueryEngine.register_table(): After registering Parquet provider, also register Memtable provider
       2. Query Merging: Use DataFusion's UNION operator or manually concatenate results
       3. Cache Access: Add public method to DataCache to retrieve batches by (catalog, schema, table)

       Key Advantages:
       - Leverages existing DataFusion infrastructure
       - Transparent to query engine
       - Automatic predicate/projection pushdown support
       - Works with both cache and Parquet seamlessly

       ---
       References to Key Code Sections

       - Cache data structures: /root/source/mynadb/src/storage/cache.rs lines 50-65, 102-115
       - RecordBatch conversion: /root/source/mynadb/src/storage/cache.rs lines 276-382
       - Flush logic: /root/source/mynadb/src/storage/cache.rs lines 386-481
       - Query execution flow: /root/source/mynadb/src/query/query_engine.rs lines 1253-1375
       - Table registration: /root/source/mynadb/src/query/query_engine.rs lines 646-743
       - TableProvider trait: /root/source/mynadb/src/query/table_provider.rs lines 109-547
       - API integration: /root/source/mynadb/src/services/api.rs lines 790-807
