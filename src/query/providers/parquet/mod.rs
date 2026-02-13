//! Custom Parquet TableProvider Implementation
//!
//! Provides a custom TableProvider for reading Parquet files with:
//! - Filter pushdown to Parquet reader
//! - Projection pushdown for column pruning
//! - Hive-style partition support (date=YYYY-MM-DD/node_id=XXX)
//! - ObjectStore integration for both local and remote storage
//!
//! This provider uses ObjectStore for ALL file operations, ensuring
//! consistent behavior across local filesystem and S3/MinIO/OSS.
//!
//! Path structure: catalog/schema/table/date=YYYY-MM-DD/node_id=XXX/file.parquet

mod config;
mod discovery;
mod exec_plan;
mod filters;
mod reader;

pub use config::{ParquetFileInfo, ParquetTableConfig};
pub use exec_plan::ParquetExecPlan;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{project_schema, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use object_store::ObjectStore;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use tracing::debug;

/// A custom TableProvider for reading Parquet files via ObjectStore
///
/// This provider handles:
/// - Schema inference from Parquet files
/// - Nested partition discovery (Hive-style date=YYYY-MM-DD/node_id=XXX)
/// - Filter and projection pushdown on both partition columns
/// - Efficient parallel file reading
///
/// All file operations use ObjectStore, supporting both local filesystem
/// and remote storage (S3/MinIO/OSS) uniformly.
pub struct ParquetTableProvider {
    /// Table schema (includes partition columns)
    schema: SchemaRef,
    /// Configuration
    config: ParquetTableConfig,
    /// Base path for the table (object store path)
    base_path: String,
    /// Object store for file operations (required)
    object_store: Arc<dyn ObjectStore>,
    /// Local parquet cache for cluster mode (optional)
    parquet_cache: Option<Arc<crate::data::cache::ParquetLocalCache>>,
    /// Hot partition cache for decoded RecordBatches (optional)
    hot_partition_cache: Option<Arc<crate::query::cache::hot_partition_cache::HotPartitionCache>>,
    /// Catalog name (for cache key)
    catalog: String,
    /// Schema name (for cache key)
    schema_name: String,
}

impl fmt::Debug for ParquetTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetTableProvider")
            .field("schema", &self.schema)
            .field("config", &self.config)
            .field("base_path", &self.base_path)
            .finish()
    }
}

impl ParquetTableProvider {
    /// Create a new provider using ObjectStore (async)
    ///
    /// This is the primary constructor. It uses ObjectStore for all file
    /// operations, supporting both local filesystem and remote storage.
    /// Schema is inferred from the first parquet file found.
    ///
    /// # Arguments
    /// * `object_store` - The ObjectStore to use for file operations
    /// * `base_path` - The base path/prefix for the table (without URL scheme)
    /// * `catalog` - Catalog name (database)
    /// * `schema_name` - Schema name
    /// * `config` - Table configuration
    pub async fn try_new(
        object_store: Arc<dyn ObjectStore>,
        base_path: impl Into<String>,
        catalog: impl Into<String>,
        schema_name: impl Into<String>,
        config: ParquetTableConfig,
    ) -> Result<Self> {
        let base_path_str = base_path.into();

        // Infer schema from ONE file
        let schema = discovery::infer_schema_from_prefix(&object_store, &base_path_str, &config).await?;

        Ok(Self {
            schema: Arc::new(schema),
            config,
            base_path: base_path_str,
            object_store,
            parquet_cache: None,
            hot_partition_cache: None,
            catalog: catalog.into(),
            schema_name: schema_name.into(),
        })
    }

    /// Create a new provider with a pre-defined schema
    ///
    /// Use this when you already know the schema and don't need inference.
    pub fn new_with_schema(
        object_store: Arc<dyn ObjectStore>,
        base_path: impl Into<String>,
        catalog: impl Into<String>,
        schema_name: impl Into<String>,
        schema: SchemaRef,
        config: ParquetTableConfig,
    ) -> Self {
        Self {
            schema,
            config,
            base_path: base_path.into(),
            object_store,
            parquet_cache: None,
            hot_partition_cache: None,
            catalog: catalog.into(),
            schema_name: schema_name.into(),
        }
    }

    /// Set local parquet cache for cluster mode
    pub fn with_parquet_cache(
        mut self,
        cache: Arc<crate::data::cache::ParquetLocalCache>,
    ) -> Self {
        self.parquet_cache = Some(cache);
        self
    }

    /// Set hot partition cache for decoded RecordBatches
    pub fn with_hot_partition_cache(
        mut self,
        cache: Arc<crate::query::cache::hot_partition_cache::HotPartitionCache>,
    ) -> Self {
        self.hot_partition_cache = Some(cache);
        self
    }

    /// Get base path
    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    /// Get configuration
    pub fn config(&self) -> &ParquetTableConfig {
        &self.config
    }

    /// Get the object store
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }
}

#[async_trait]
impl TableProvider for ParquetTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // We support inexact filter pushdown for all filters
        // Partition filters are exact, others are pushed to Parquet reader
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Compute the projected schema
        let projected_schema = project_schema(&self.schema, projection)?;

        // Discover files with partition filter pruning
        let files =
            discovery::discover_files(&self.object_store, &self.base_path, &self.config, filters)
                .await?;

        debug!(
            "ParquetTableProvider scan: table={}, files={}, filters={}, projection={:?}",
            self.config.table_name,
            files.len(),
            filters.len(),
            projection
        );

        // Create the execution plan
        // Pass both projected_schema (for output) and full schema (for caching full batches)
        let exec = exec_plan::ParquetExecPlan::new(
            projected_schema,
            self.schema.clone(),
            files,
            projection.cloned(),
            filters.to_vec(),
            limit,
            self.config.clone(),
            Arc::clone(&self.object_store),
            self.parquet_cache.clone(),
            self.hot_partition_cache.clone(),
            self.catalog.clone(),
            self.schema_name.clone(),
            self.config.table_name.clone(),
        );

        Ok(Arc::new(exec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionContext;
    use object_store::local::LocalFileSystem;
    use std::fs::File;
    use std::path::Path;
    use tempfile::TempDir;

    fn create_test_parquet(dir: &Path, filename: &str, id_start: i64) -> std::path::PathBuf {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let ids = Int64Array::from(vec![id_start, id_start + 1, id_start + 2]);
        let names = StringArray::from(vec![
            Some(format!("name_{}", id_start)),
            Some(format!("name_{}", id_start + 1)),
            None,
        ]);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)]).unwrap();

        let path = dir.join(filename);
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        path
    }

    fn create_local_object_store(path: &Path) -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix(path).unwrap())
    }

    #[tokio::test]
    async fn test_discover_nested_partitions() {
        // Test nested partitions: date=YYYY-MM-DD/node_id=XXX
        let temp_dir = TempDir::new().unwrap();
        let part = temp_dir.path().join("date=2024-01-01").join("node_id=data-1");
        std::fs::create_dir_all(&part).unwrap();
        create_test_parquet(&part, "file1.parquet", 1);
        create_test_parquet(&part, "file2.parquet", 10);

        let object_store = create_local_object_store(temp_dir.path());
        let config = ParquetTableConfig::new("test");
        let provider = ParquetTableProvider::try_new(object_store, "", "testdb", "default", config)
            .await
            .unwrap();

        // Schema should include date and node_id partition columns + 2 data columns (id, name)
        assert_eq!(provider.schema().fields().len(), 4);
        assert!(provider.schema().field_with_name("date").is_ok());
        assert!(provider.schema().field_with_name("node_id").is_ok());
    }

    #[tokio::test]
    async fn test_discover_multi_node_partitions() {
        let temp_dir = TempDir::new().unwrap();

        // Create nested partition structure with multiple nodes
        let part1_node1 = temp_dir.path().join("date=2024-01-01").join("node_id=data-1");
        let part1_node2 = temp_dir.path().join("date=2024-01-01").join("node_id=data-2");
        let part2_node1 = temp_dir.path().join("date=2024-01-02").join("node_id=data-1");
        std::fs::create_dir_all(&part1_node1).unwrap();
        std::fs::create_dir_all(&part1_node2).unwrap();
        std::fs::create_dir_all(&part2_node1).unwrap();

        create_test_parquet(&part1_node1, "data.parquet", 1);
        create_test_parquet(&part1_node2, "data.parquet", 10);
        create_test_parquet(&part2_node1, "data.parquet", 20);

        let object_store = create_local_object_store(temp_dir.path());
        let config = ParquetTableConfig::new("test");
        let provider = ParquetTableProvider::try_new(object_store, "", "testdb", "default", config)
            .await
            .unwrap();

        // Schema should include both partition columns
        assert!(provider.schema().field_with_name("date").is_ok());
        assert!(provider.schema().field_with_name("node_id").is_ok());
    }

    #[tokio::test]
    async fn test_query_nested_partitions() {
        let temp_dir = TempDir::new().unwrap();

        let part = temp_dir.path().join("date=2024-01-01").join("node_id=data-1");
        std::fs::create_dir_all(&part).unwrap();
        create_test_parquet(&part, "data.parquet", 1);

        let object_store = create_local_object_store(temp_dir.path());
        let config = ParquetTableConfig::new("test_table");
        let provider = ParquetTableProvider::try_new(object_store, "", "testdb", "default", config)
            .await
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT id, date, node_id FROM test").await.unwrap();
        let results = df.collect().await.unwrap();

        assert!(!results.is_empty());
        // Should have id, date, and node_id columns
        assert_eq!(results[0].num_columns(), 3);
    }

    #[tokio::test]
    async fn test_query_with_node_id_filter() {
        let temp_dir = TempDir::new().unwrap();

        // Create data for two nodes
        let part_node1 = temp_dir.path().join("date=2024-01-01").join("node_id=data-1");
        let part_node2 = temp_dir.path().join("date=2024-01-01").join("node_id=data-2");
        std::fs::create_dir_all(&part_node1).unwrap();
        std::fs::create_dir_all(&part_node2).unwrap();
        create_test_parquet(&part_node1, "data.parquet", 1);
        create_test_parquet(&part_node2, "data.parquet", 100);

        let object_store = create_local_object_store(temp_dir.path());
        let config = ParquetTableConfig::new("test_table");
        let provider = ParquetTableProvider::try_new(object_store, "", "testdb", "default", config)
            .await
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        // Query with node_id filter - should only get data-1's rows
        let df = ctx
            .sql("SELECT id, node_id FROM test WHERE node_id = 'data-1'")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        assert!(!results.is_empty());
        // All rows should have node_id = 'data-1'
        let node_id_col = results[0].column(1);
        let node_ids = node_id_col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..node_ids.len() {
            assert_eq!(node_ids.value(i), "data-1");
        }
    }

    /// Test that schema inference merges fields from files with different column sets.
    /// Simulates Telegraf behavior where e.g. `swap` sends `free,total,used` in one
    /// batch and `in,out` in another, resulting in parquet files with disjoint schemas.
    #[tokio::test]
    async fn test_schema_merges_heterogeneous_parquet_files() {
        use datafusion::arrow::array::Float64Array;

        let temp_dir = TempDir::new().unwrap();
        let part = temp_dir.path().join("date=2024-01-01").join("node_id=standalone");
        std::fs::create_dir_all(&part).unwrap();

        // File 1: fields {time, host, free, total, used}
        {
            let schema = Arc::new(Schema::new(vec![
                Field::new("time", DataType::Int64, false),
                Field::new("host", DataType::Utf8, true),
                Field::new("free", DataType::Float64, true),
                Field::new("total", DataType::Float64, true),
                Field::new("used", DataType::Float64, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1000])),
                    Arc::new(StringArray::from(vec!["host1"])),
                    Arc::new(Float64Array::from(vec![100.0])),
                    Arc::new(Float64Array::from(vec![200.0])),
                    Arc::new(Float64Array::from(vec![100.0])),
                ],
            )
            .unwrap();
            let path = part.join("L0_aaa.parquet");
            let file = File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        // File 2: fields {time, host, swap_in, swap_out} (different field set)
        {
            let schema = Arc::new(Schema::new(vec![
                Field::new("time", DataType::Int64, false),
                Field::new("host", DataType::Utf8, true),
                Field::new("swap_in", DataType::Float64, true),
                Field::new("swap_out", DataType::Float64, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1000])),
                    Arc::new(StringArray::from(vec!["host1"])),
                    Arc::new(Float64Array::from(vec![512.0])),
                    Arc::new(Float64Array::from(vec![256.0])),
                ],
            )
            .unwrap();
            let path = part.join("L0_bbb.parquet");
            let file = File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let object_store = create_local_object_store(temp_dir.path());
        let config = ParquetTableConfig::new("test_swap");
        let provider = ParquetTableProvider::try_new(object_store, "", "testdb", "default", config)
            .await
            .unwrap();

        let schema = provider.schema();

        // Should have ALL fields from both files + partition columns
        // time, host, free, total, used, swap_in, swap_out, date, node_id = 9
        assert_eq!(schema.fields().len(), 9, "schema fields: {:?}", schema);
        assert!(schema.field_with_name("time").is_ok());
        assert!(schema.field_with_name("host").is_ok());
        assert!(schema.field_with_name("free").is_ok());
        assert!(schema.field_with_name("total").is_ok());
        assert!(schema.field_with_name("used").is_ok());
        assert!(schema.field_with_name("swap_in").is_ok());
        assert!(schema.field_with_name("swap_out").is_ok());
        assert!(schema.field_with_name("date").is_ok());
        assert!(schema.field_with_name("node_id").is_ok());

        // Query should return rows from both files with NULLs for missing columns
        let ctx = SessionContext::new();
        ctx.register_table("swap", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT * FROM swap ORDER BY free DESC NULLS LAST").await.unwrap();
        let results = df.collect().await.unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "should have 2 rows (1 from each file)");
    }

    #[tokio::test]
    async fn test_query_with_projection() {
        let temp_dir = TempDir::new().unwrap();

        let part = temp_dir.path().join("date=2024-01-01").join("node_id=data-1");
        std::fs::create_dir_all(&part).unwrap();
        create_test_parquet(&part, "data.parquet", 1);

        let object_store = create_local_object_store(temp_dir.path());
        let config = ParquetTableConfig::new("test_table");
        let provider = ParquetTableProvider::try_new(object_store, "", "testdb", "default", config)
            .await
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT name FROM test").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results[0].num_columns(), 1);
    }
}
