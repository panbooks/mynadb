//! Custom Parquet TableProvider Implementation
//!
//! Provides a custom TableProvider for reading Parquet files with:
//! - Filter pushdown to Parquet reader
//! - Projection pushdown for column pruning
//! - Hive-style partition support (date=YYYY-MM-DD)
//! - Integration with DataFusion's execution engine
//!
//! This replaces the built-in ListingTable and register_parquet() approaches
//! to provide more control over the query execution path.

use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{project_schema, DataFusionError, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::ExecutionMode;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::Expr;
use futures::stream::{self, StreamExt, TryStreamExt};
use futures::Stream;
use object_store::ObjectStore;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tracing::debug;

/// Configuration for the Parquet table provider
///
/// All tables are partitioned by date (Hive-style: date=YYYY-MM-DD)
#[derive(Debug, Clone)]
pub struct ParquetTableConfig {
    /// Enable filter pushdown to Parquet reader
    pub filter_pushdown: bool,
    /// Enable projection pushdown for column pruning
    pub projection_pushdown: bool,
    /// Table name for display purposes
    pub table_name: String,
    /// Batch size for reading
    pub batch_size: usize,
    /// Partition column name (default: "date")
    pub partition_column: String,
}

impl Default for ParquetTableConfig {
    fn default() -> Self {
        Self {
            filter_pushdown: true,
            projection_pushdown: true,
            table_name: "parquet_table".to_string(),
            batch_size: 8192,
            partition_column: "date".to_string(),
        }
    }
}

impl ParquetTableConfig {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            ..Default::default()
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

/// Information about a Parquet file to read
#[derive(Debug, Clone)]
pub struct ParquetFileInfo {
    /// Full path to the Parquet file
    pub path: PathBuf,
    /// Partition values (e.g., {"date": "2024-01-15"})
    pub partition_values: HashMap<String, String>,
    /// File size in bytes (for statistics)
    pub file_size: u64,
}

/// A custom TableProvider for reading Parquet files
///
/// This provider handles:
/// - Schema inference from Parquet files
/// - Partition discovery (Hive-style date=YYYY-MM-DD)
/// - Filter and projection pushdown
/// - Efficient parallel file reading
pub struct ParquetTableProvider {
    /// Table schema (includes partition columns)
    schema: SchemaRef,
    /// List of Parquet files to read
    files: RwLock<Vec<ParquetFileInfo>>,
    /// Configuration
    config: ParquetTableConfig,
    /// Base path for the table
    base_path: PathBuf,
    /// Object store for remote storage (optional)
    object_store: Option<Arc<dyn ObjectStore>>,
    /// Local parquet cache for cluster mode (optional)
    parquet_cache: Option<Arc<crate::data::cache::ParquetLocalCache>>,
}

impl fmt::Debug for ParquetTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetTableProvider")
            .field("schema", &self.schema)
            .field("config", &self.config)
            .field("base_path", &self.base_path)
            .field("file_count", &self.files.read().len())
            .finish()
    }
}

impl ParquetTableProvider {
    /// Create a new provider by scanning the table directory
    pub fn try_new(base_path: impl AsRef<Path>, config: ParquetTableConfig) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();

        // Discover files and infer schema
        let (files, schema) = Self::discover_files_and_schema(&base_path, &config)?;

        Ok(Self {
            schema: Arc::new(schema),
            files: RwLock::new(files),
            config,
            base_path,
            object_store: None,
            parquet_cache: None,
        })
    }

    /// Create a new provider with a pre-defined schema
    pub fn new_with_schema(
        base_path: impl AsRef<Path>,
        schema: SchemaRef,
        config: ParquetTableConfig,
    ) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        let files = Self::discover_files(&base_path, &config)?;

        Ok(Self {
            schema,
            files: RwLock::new(files),
            config,
            base_path,
            object_store: None,
            parquet_cache: None,
        })
    }

    /// Create provider with object store for remote storage
    pub fn with_object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(store);
        self
    }

    /// Set local parquet cache for cluster mode
    pub fn with_parquet_cache(mut self, cache: Arc<crate::data::cache::ParquetLocalCache>) -> Self {
        self.parquet_cache = Some(cache);
        self
    }

    /// Create a new provider using ObjectStore (async, lazy file discovery)
    ///
    /// This is the unified constructor that works with both local filesystem
    /// and remote object storage (S3/MinIO/OSS). File discovery is deferred
    /// to scan time for better performance with large tables.
    ///
    /// # Arguments
    /// * `object_store` - The ObjectStore to use for file operations
    /// * `base_path` - The base path/prefix for the table (without URL scheme)
    /// * `config` - Table configuration
    pub async fn try_new_async(
        object_store: Arc<dyn ObjectStore>,
        base_path: impl Into<String>,
        config: ParquetTableConfig,
    ) -> Result<Self> {
        let base_path_str = base_path.into();
        let base_path_buf = PathBuf::from(&base_path_str);

        // Only infer schema from ONE file - don't list all files
        let schema = Self::infer_schema_from_prefix(&object_store, &base_path_str, &config).await?;

        Ok(Self {
            schema: Arc::new(schema),
            files: RwLock::new(Vec::new()), // Empty - lazy discovery at scan time
            config,
            base_path: base_path_buf,
            object_store: Some(object_store),
            parquet_cache: None,
        })
    }

    /// Infer schema by finding ONE parquet file under the prefix
    async fn infer_schema_from_prefix(
        object_store: &Arc<dyn ObjectStore>,
        base_path: &str,
        config: &ParquetTableConfig,
    ) -> Result<Schema> {
        use futures::StreamExt;
        use object_store::path::Path as ObjectPath;

        let prefix = ObjectPath::from(base_path.trim_start_matches('/'));

        // Stream through files until we find one .parquet file
        let mut list_stream = object_store.list(Some(&prefix));

        while let Some(result) = list_stream.next().await {
            match result {
                Ok(meta) => {
                    let path_str = meta.location.to_string();
                    if path_str.ends_with(".parquet") {
                        // Found a parquet file, infer schema from it
                        let schema = Self::infer_schema_from_path(object_store, &meta.location).await?;

                        // Add partition column to schema
                        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
                        if !fields.iter().any(|f| f.name() == &config.partition_column) {
                            fields.push(Field::new(&config.partition_column, DataType::Utf8, true));
                        }
                        return Ok(Schema::new(fields));
                    }
                }
                Err(e) => {
                    return Err(DataFusionError::External(Box::new(e)));
                }
            }
        }

        // No parquet files found - return minimal schema with partition column
        Ok(Schema::new(vec![Field::new(&config.partition_column, DataType::Utf8, true)]))
    }

    /// Infer schema from a specific parquet file path
    async fn infer_schema_from_path(
        object_store: &Arc<dyn ObjectStore>,
        path: &object_store::path::Path,
    ) -> Result<Schema> {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // Fetch the file content
        let get_result = object_store.get(path).await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let bytes = get_result.bytes().await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Build reader and get schema
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(reader.schema().as_ref().clone())
    }

    /// Discover files for scan with partition filter pruning (lazy discovery)
    ///
    /// This is called at scan time, not construction time. It uses partition
    /// filters to prune directories before listing files, dramatically reducing
    /// the number of list operations for large tables.
    async fn discover_files_for_scan(
        object_store: &Arc<dyn ObjectStore>,
        base_path: &str,
        config: &ParquetTableConfig,
        filters: &[Expr],
    ) -> Result<Vec<ParquetFileInfo>> {
        use object_store::path::Path as ObjectPath;
        use futures::TryStreamExt;

        let base_prefix = ObjectPath::from(base_path.trim_start_matches('/'));

        // Extract partition filters for pruning
        let partition_filters = Self::extract_partition_filters(&config.partition_column, filters);

        // List partition directories using list_with_delimiter
        let list_result = object_store.list_with_delimiter(Some(&base_prefix)).await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut files = Vec::new();
        let partition_prefix = format!("{}=", config.partition_column);

        // Process each partition directory
        for prefix in list_result.common_prefixes {
            let dir_name = prefix.as_ref()
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .unwrap_or("");

            // Check if this is a partition directory
            if !dir_name.starts_with(&partition_prefix) {
                continue;
            }

            let partition_value = dir_name
                .strip_prefix(&partition_prefix)
                .unwrap_or("")
                .to_string();

            // Apply partition filter pruning
            if !partition_filters.is_empty() && !Self::matches_partition_filter(&partition_value, &partition_filters) {
                debug!(
                    "Pruning partition {} (doesn't match filters)",
                    partition_value
                );
                continue;
            }

            // List files in this partition
            let partition_files = object_store.list(Some(&prefix));
            let objects: Vec<_> = partition_files.try_collect().await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            for meta in objects {
                let path_str = meta.location.to_string();
                if !path_str.ends_with(".parquet") {
                    continue;
                }

                let mut partition_values = HashMap::new();
                partition_values.insert(config.partition_column.clone(), partition_value.clone());

                files.push(ParquetFileInfo {
                    path: PathBuf::from(&path_str),
                    partition_values,
                    file_size: meta.size as u64,
                });
            }
        }

        files.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(files)
    }

    /// Discover Parquet files in the table directory (sync, local filesystem only)
    /// All tables are partitioned by date (Hive-style: date=YYYY-MM-DD)
    fn discover_files(
        base_path: &Path,
        config: &ParquetTableConfig,
    ) -> Result<Vec<ParquetFileInfo>> {
        let mut files = Vec::new();

        if !base_path.exists() {
            return Ok(files);
        }

        // Scan for Hive-style partitions (date=YYYY-MM-DD)
        Self::scan_partitioned_dir(base_path, config, &mut files)?;

        files.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(files)
    }

    /// Scan a partitioned directory structure
    fn scan_partitioned_dir(
        base_path: &Path,
        config: &ParquetTableConfig,
        files: &mut Vec<ParquetFileInfo>,
    ) -> Result<()> {
        let entries =
            std::fs::read_dir(base_path).map_err(|e| DataFusionError::External(Box::new(e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                    // Check for partition format: column=value
                    let prefix = format!("{}=", config.partition_column);
                    if dir_name.starts_with(&prefix) {
                        let partition_value =
                            dir_name.strip_prefix(&prefix).unwrap_or("").to_string();

                        // Scan Parquet files in this partition
                        Self::scan_partition_files(
                            &path,
                            &config.partition_column,
                            &partition_value,
                            files,
                        )?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Scan Parquet files within a partition directory
    fn scan_partition_files(
        partition_path: &Path,
        partition_column: &str,
        partition_value: &str,
        files: &mut Vec<ParquetFileInfo>,
    ) -> Result<()> {
        let entries = std::fs::read_dir(partition_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let path = entry.path();

            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "parquet" {
                        let metadata = std::fs::metadata(&path)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        let mut partition_values = HashMap::new();
                        partition_values
                            .insert(partition_column.to_string(), partition_value.to_string());

                        files.push(ParquetFileInfo {
                            path,
                            partition_values,
                            file_size: metadata.len(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Discover files and infer schema from the first Parquet file
    fn discover_files_and_schema(
        base_path: &Path,
        config: &ParquetTableConfig,
    ) -> Result<(Vec<ParquetFileInfo>, Schema)> {
        let files = Self::discover_files(base_path, config)?;

        if files.is_empty() {
            // Return minimal schema with partition column
            let schema = Schema::new(vec![Field::new(
                &config.partition_column,
                DataType::Utf8,
                true,
            )]);
            return Ok((files, schema));
        }

        // Read schema from first file
        let first_file = &files[0];
        let file =
            File::open(&first_file.path).map_err(|e| DataFusionError::External(Box::new(e)))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let base_schema = reader.schema().as_ref().clone();

        // Add partition column to schema
        let partition_field = Field::new(&config.partition_column, DataType::Utf8, true);
        let mut fields: Vec<Field> =
            base_schema.fields().iter().map(|f| f.as_ref().clone()).collect();

        // Only add if not already present
        if !fields.iter().any(|f| f.name() == &config.partition_column) {
            fields.push(partition_field);
        }
        let schema = Schema::new(fields);

        Ok((files, schema))
    }

    /// Refresh file list (re-scan directory)
    pub fn refresh_files(&self) -> Result<()> {
        let files = Self::discover_files(&self.base_path, &self.config)?;
        *self.files.write() = files;
        Ok(())
    }

    /// Get the number of files
    pub fn file_count(&self) -> usize {
        self.files.read().len()
    }

    /// Get total size of all files
    pub fn total_size(&self) -> u64 {
        self.files.read().iter().map(|f| f.file_size).sum()
    }

    /// Get base path
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    /// Get configuration
    pub fn config(&self) -> &ParquetTableConfig {
        &self.config
    }

    /// Filter files based on partition predicates
    fn filter_files_by_partition(
        files: &[ParquetFileInfo],
        partition_column: &str,
        filters: &[Expr],
    ) -> Vec<ParquetFileInfo> {
        // Extract partition filter values from expressions
        let partition_filter_values = Self::extract_partition_filters(partition_column, filters);

        if partition_filter_values.is_empty() {
            // No partition filters, return all files
            return files.to_vec();
        }

        // Filter files based on partition values
        files
            .iter()
            .filter(|f| {
                if let Some(value) = f.partition_values.get(partition_column) {
                    Self::matches_partition_filter(value, &partition_filter_values)
                } else {
                    true // Include files without partition values
                }
            })
            .cloned()
            .collect()
    }

    /// Extract partition filter conditions from expressions
    fn extract_partition_filters(partition_column: &str, filters: &[Expr]) -> Vec<PartitionFilter> {
        let mut partition_filters = Vec::new();

        for filter in filters {
            Self::extract_partition_filter_from_expr(
                partition_column,
                filter,
                &mut partition_filters,
            );
        }

        partition_filters
    }

    /// Recursively extract partition filters from an expression
    fn extract_partition_filter_from_expr(
        partition_column: &str,
        expr: &Expr,
        filters: &mut Vec<PartitionFilter>,
    ) {
        if let Expr::BinaryExpr(binary) = expr {
            // Check if this is a comparison on the partition column
            if let Expr::Column(col) = binary.left.as_ref() {
                if col.name == partition_column {
                    if let Expr::Literal(scalar) = binary.right.as_ref() {
                        if let Some(value) = scalar
                            .to_string()
                            .strip_prefix("Utf8(\"")
                            .and_then(|s| s.strip_suffix("\")"))
                        {
                            let filter = match binary.op {
                                datafusion::logical_expr::Operator::Eq => {
                                    PartitionFilter::Eq(value.to_string())
                                }
                                datafusion::logical_expr::Operator::Gt => {
                                    PartitionFilter::Gt(value.to_string())
                                }
                                datafusion::logical_expr::Operator::GtEq => {
                                    PartitionFilter::GtEq(value.to_string())
                                }
                                datafusion::logical_expr::Operator::Lt => {
                                    PartitionFilter::Lt(value.to_string())
                                }
                                datafusion::logical_expr::Operator::LtEq => {
                                    PartitionFilter::LtEq(value.to_string())
                                }
                                _ => return,
                            };
                            filters.push(filter);
                        }
                    }
                }
            }

            // Recurse into AND expressions
            if binary.op == datafusion::logical_expr::Operator::And {
                Self::extract_partition_filter_from_expr(partition_column, &binary.left, filters);
                Self::extract_partition_filter_from_expr(partition_column, &binary.right, filters);
            }
        }
    }

    /// Check if a partition value matches the filter
    fn matches_partition_filter(value: &str, filters: &[PartitionFilter]) -> bool {
        for filter in filters {
            let matches = match filter {
                PartitionFilter::Eq(v) => value == v,
                PartitionFilter::Gt(v) => value > v.as_str(),
                PartitionFilter::GtEq(v) => value >= v.as_str(),
                PartitionFilter::Lt(v) => value < v.as_str(),
                PartitionFilter::LtEq(v) => value <= v.as_str(),
            };
            if !matches {
                return false;
            }
        }
        true
    }
}

/// Partition filter condition
#[derive(Debug, Clone)]
enum PartitionFilter {
    Eq(String),
    Gt(String),
    GtEq(String),
    Lt(String),
    LtEq(String),
}

/// Extracted equality predicate for bloom filter optimization
#[derive(Debug, Clone)]
struct EqualityPredicate {
    /// Column name
    column: String,
    /// Value to match (for string columns)
    value: String,
}

/// Extract equality predicates from DataFusion expressions
/// These are the predicates that can benefit from bloom filter checking
fn extract_equality_predicates(filters: &[Expr], schema: &SchemaRef) -> Vec<EqualityPredicate> {
    let mut predicates = Vec::new();

    for filter in filters {
        extract_equality_from_expr(filter, schema, &mut predicates);
    }

    predicates
}

/// Recursively extract equality predicates from an expression
fn extract_equality_from_expr(
    expr: &Expr,
    schema: &SchemaRef,
    predicates: &mut Vec<EqualityPredicate>,
) {
    match expr {
        Expr::BinaryExpr(binary) => {
            // Handle equality: column = 'value'
            if binary.op == datafusion::logical_expr::Operator::Eq {
                if let (Expr::Column(col), Expr::Literal(scalar)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                {
                    // Only extract for UTF8 columns (bloom filter targets)
                    if let Some(field) = schema.field_with_name(&col.name).ok() {
                        if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
                            if let Some(value) = scalar_to_string(scalar) {
                                predicates.push(EqualityPredicate {
                                    column: col.name.clone(),
                                    value,
                                });
                            }
                        }
                    }
                }
                // Handle reversed: 'value' = column
                if let (Expr::Literal(scalar), Expr::Column(col)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                {
                    if let Some(field) = schema.field_with_name(&col.name).ok() {
                        if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
                            if let Some(value) = scalar_to_string(scalar) {
                                predicates.push(EqualityPredicate {
                                    column: col.name.clone(),
                                    value,
                                });
                            }
                        }
                    }
                }
            }
            // Recurse into AND expressions
            if binary.op == datafusion::logical_expr::Operator::And {
                extract_equality_from_expr(&binary.left, schema, predicates);
                extract_equality_from_expr(&binary.right, schema, predicates);
            }
        }
        _ => {}
    }
}

/// Convert a scalar value to string
fn scalar_to_string(scalar: &datafusion::common::ScalarValue) -> Option<String> {
    match scalar {
        datafusion::common::ScalarValue::Utf8(Some(s)) => Some(s.clone()),
        datafusion::common::ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => {
            // Try to extract from the debug format for other representations
            let s = scalar.to_string();
            if let Some(value) = s
                .strip_prefix("Utf8(\"")
                .and_then(|s| s.strip_suffix("\")"))
            {
                return Some(value.to_string());
            }
            None
        }
    }
}

/// Create a row filter from equality predicates for Parquet reading
/// This enables row-level filtering with potential bloom filter benefits
fn create_row_filter_from_predicates(
    predicates: Vec<EqualityPredicate>,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> Option<RowFilter> {
    if predicates.is_empty() {
        return None;
    }

    let mut arrow_predicates: Vec<Box<dyn ArrowPredicate>> = Vec::new();

    for pred in predicates {
        // Find the column index in the parquet schema
        let col_idx = (0..parquet_schema.num_columns())
            .find(|&i| parquet_schema.column(i).name() == pred.column);

        if let Some(idx) = col_idx {
            let projection =
                datafusion::parquet::arrow::ProjectionMask::roots(parquet_schema, vec![idx]);
            let expected_value = pred.value.clone();

            let predicate_fn = ArrowPredicateFn::new(projection, move |batch: RecordBatch| {
                if batch.num_columns() == 0 {
                    return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
                }

                let col = batch.column(0);

                // Handle different string array types
                if let Some(string_array) = col.as_any().downcast_ref::<StringArray>() {
                    let result: BooleanArray = string_array
                        .iter()
                        .map(|opt_val| opt_val.map(|v| v == expected_value))
                        .collect();
                    Ok(result)
                } else if let Some(string_view_array) =
                    col.as_any()
                        .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                {
                    let result: BooleanArray = string_view_array
                        .iter()
                        .map(|opt_val| opt_val.map(|v| v == expected_value))
                        .collect();
                    Ok(result)
                } else if let Some(large_string_array) =
                    col.as_any()
                        .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
                {
                    let result: BooleanArray = large_string_array
                        .iter()
                        .map(|opt_val| opt_val.map(|v| v == expected_value))
                        .collect();
                    Ok(result)
                } else {
                    // Column type doesn't match, return all true (don't filter)
                    Ok(BooleanArray::from(vec![true; batch.num_rows()]))
                }
            });

            arrow_predicates.push(Box::new(predicate_fn));
        }
    }

    if arrow_predicates.is_empty() {
        return None;
    }

    Some(RowFilter::new(arrow_predicates))
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

        // Get files - use lazy discovery for object store, or cached files for local
        let files = if let Some(ref object_store) = self.object_store {
            // Lazy discovery at scan time with partition filter pruning
            let base_path = self.base_path.to_string_lossy().to_string();
            Self::discover_files_for_scan(object_store, &base_path, &self.config, filters).await?
        } else {
            // Use pre-discovered files (local filesystem) with partition filter pruning
            let all_files = self.files.read().clone();
            Self::filter_files_by_partition(&all_files, &self.config.partition_column, filters)
        };

        debug!(
            "ParquetTableProvider scan: table={}, files={}, filters={}, projection={:?}",
            self.config.table_name,
            files.len(),
            filters.len(),
            projection
        );

        // Create the execution plan
        let exec = ParquetExecPlan::new(
            projected_schema,
            files,
            projection.cloned(),
            filters.to_vec(),
            limit,
            self.config.clone(),
            self.parquet_cache.clone(),
        );

        Ok(Arc::new(exec))
    }
}

/// Custom execution plan for reading Parquet files
pub struct ParquetExecPlan {
    /// Schema after projection
    schema: SchemaRef,
    /// Files to read
    files: Vec<ParquetFileInfo>,
    /// Column projection (indices into original schema)
    projection: Option<Vec<usize>>,
    /// Pushed-down filters
    filters: Vec<Expr>,
    /// Optional row limit
    limit: Option<usize>,
    /// Configuration
    config: ParquetTableConfig,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties
    properties: PlanProperties,
    /// Local parquet cache for cluster mode (optional)
    parquet_cache: Option<Arc<crate::data::cache::ParquetLocalCache>>,
}

impl fmt::Debug for ParquetExecPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetExecPlan")
            .field("schema", &self.schema)
            .field("files", &self.files.len())
            .field("projection", &self.projection)
            .field("filters", &self.filters.len())
            .field("limit", &self.limit)
            .field("config", &self.config)
            .field("parquet_cache", &self.parquet_cache.is_some())
            .finish()
    }
}

impl ParquetExecPlan {
    pub fn new(
        schema: SchemaRef,
        files: Vec<ParquetFileInfo>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        config: ParquetTableConfig,
        parquet_cache: Option<Arc<crate::data::cache::ParquetLocalCache>>,
    ) -> Self {
        // Determine partitioning based on number of files
        let num_partitions = files.len().max(1);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(num_partitions),
            ExecutionMode::Bounded,
        );

        Self {
            schema,
            files,
            projection,
            filters,
            limit,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            parquet_cache,
        }
    }

    /// Get file count
    pub fn file_count(&self) -> usize {
        self.files.len()
    }
}

impl DisplayAs for ParquetExecPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ParquetExecPlan: table={}, files={}, projection={:?}, filters={}, limit={:?}",
                    self.config.table_name,
                    self.files.len(),
                    self.projection,
                    self.filters.len(),
                    self.limit
                )
            }
        }
    }
}

impl ExecutionPlan for ParquetExecPlan {
    fn name(&self) -> &str {
        "ParquetExecPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // Leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // For single partition, read all files
        // For multiple partitions, each partition reads one file
        let files_to_read = if self.files.len() <= 1 || partition == 0 {
            self.files.clone()
        } else if partition < self.files.len() {
            vec![self.files[partition].clone()]
        } else {
            vec![]
        };

        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let batch_size = self.config.batch_size;
        let limit = self.limit;
        let partition_column = self.config.partition_column.clone();
        let filters = self.filters.clone();
        let parquet_cache = self.parquet_cache.clone();

        // Create async stream that reads files
        let stream = stream::iter(files_to_read)
            .map(move |file_info| {
                let schema = schema.clone();
                let projection = projection.clone();
                let partition_column = partition_column.clone();
                let filters = filters.clone();
                let parquet_cache = parquet_cache.clone();

                async move {
                    read_parquet_file(
                        &file_info,
                        schema,
                        projection,
                        batch_size,
                        limit,
                        &partition_column,
                        &filters,
                        parquet_cache.as_ref(),
                    )
                    .await
                }
            })
            .buffered(4) // Read up to 4 files concurrently
            .try_flatten();

        let stream = RecordBatchStreamAdapter::new(self.schema.clone(), stream);
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Read a single Parquet file and return batches
///
/// Parameters:
/// - `file_info`: Information about the Parquet file to read
/// - `target_schema`: The projected schema (already subset based on projection)
/// - `_projection`: Original projection indices (unused - we use target_schema instead)
/// - `batch_size`: Batch size for reading
/// - `limit`: Optional row limit
/// - `partition_column`: Name of the partition column (default: "date")
/// - `filters`: Pushed-down filter expressions for bloom filter and predicate pushdown
/// - `parquet_cache`: Optional local cache for remote files (cluster mode)
async fn read_parquet_file(
    file_info: &ParquetFileInfo,
    target_schema: SchemaRef,
    _projection: Option<Vec<usize>>,
    batch_size: usize,
    limit: Option<usize>,
    partition_column: &str,
    filters: &[Expr],
    parquet_cache: Option<&Arc<crate::data::cache::ParquetLocalCache>>,
) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
    // Get local file path - either from cache (if remote) or use original path (if local)
    let local_path = if let Some(cache) = parquet_cache {
        let path_str = file_info.path.to_string_lossy();
        match cache.get_or_fetch(&path_str).await {
            Ok(cached_path) => cached_path,
            Err(e) => {
                // Cache fetch failed - fall back to original path
                debug!(
                    "Parquet cache fetch failed for {}: {}, using original path",
                    path_str, e
                );
                file_info.path.clone()
            }
        }
    } else {
        file_info.path.clone()
    };

    let file = File::open(&local_path).map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .with_batch_size(batch_size);

    // Build projection based on target schema field names
    // Map field names to file column indices
    let file_schema = builder.schema().clone();
    let file_projection: Vec<usize> = target_schema
        .fields()
        .iter()
        .filter(|field| {
            // Exclude partition column - it will be added from partition path
            field.name() != partition_column
        })
        .filter_map(|field| {
            file_schema
                .fields()
                .iter()
                .position(|f| f.name() == field.name())
        })
        .collect();

    if !file_projection.is_empty() {
        let projection_mask = datafusion::parquet::arrow::ProjectionMask::roots(
            builder.parquet_schema(),
            file_projection.clone(),
        );
        builder = builder.with_projection(projection_mask);
    }

    // Apply limit if specified
    if let Some(lim) = limit {
        builder = builder.with_limit(lim);
    }

    // Extract equality predicates for bloom filter optimization
    // These predicates on string columns will benefit from bloom filter checks
    let equality_predicates = extract_equality_predicates(filters, &file_schema);

    // Apply row filter for predicate pushdown
    // The parquet reader will use bloom filters (if available) during row group selection
    // and the row filter provides additional row-level filtering
    if !equality_predicates.is_empty() {
        let parquet_schema = builder.parquet_schema();
        if let Some(row_filter) =
            create_row_filter_from_predicates(equality_predicates.clone(), parquet_schema)
        {
            debug!(
                "Applying {} equality predicates to Parquet reader for bloom filter/predicate pushdown: {:?}",
                equality_predicates.len(),
                file_info.path
            );
            builder = builder.with_row_filter(row_filter);
        }
    }

    let reader = builder
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Clone partition values for the stream
    let partition_value = file_info.partition_values.get(partition_column).cloned();

    // Convert to stream and add partition column if needed
    let batches: Vec<RecordBatch> = reader.filter_map(|r| r.ok()).collect();

    let result_batches: Vec<Result<RecordBatch>> = batches
        .into_iter()
        .map(|batch| {
            add_partition_column(
                &batch,
                &target_schema,
                partition_column,
                partition_value.as_deref(),
            )
        })
        .collect();

    Ok(Box::pin(stream::iter(result_batches)))
}

/// Add partition column to a record batch
fn add_partition_column(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
    partition_column: &str,
    partition_value: Option<&str>,
) -> Result<RecordBatch> {
    use datafusion::arrow::array::StringArray;

    // Check if partition column is in target schema
    let has_partition_col = target_schema
        .fields()
        .iter()
        .any(|f| f.name() == partition_column);

    if !has_partition_col {
        // Partition column not requested in projection
        // Just return the batch with the target schema if they match
        if batch.schema().fields().len() == target_schema.fields().len() {
            return Ok(batch.clone());
        }
        // Reproject batch to match target schema
        let columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = target_schema
            .fields()
            .iter()
            .filter_map(|field| {
                batch
                    .schema()
                    .index_of(field.name())
                    .ok()
                    .map(|idx| batch.column(idx).clone())
            })
            .collect();

        if columns.len() == target_schema.fields().len() {
            return RecordBatch::try_new(target_schema.clone(), columns)
                .map_err(|e| DataFusionError::External(Box::new(e)));
        }
        return Ok(batch.clone());
    }

    // Build new columns array with partition column in correct position
    let mut columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = Vec::new();
    let num_rows = batch.num_rows();

    for field in target_schema.fields() {
        if field.name() == partition_column {
            // Add partition column with the partition value
            let value = partition_value.unwrap_or("");
            let array: Arc<dyn datafusion::arrow::array::Array> =
                Arc::new(StringArray::from(vec![value; num_rows]));
            columns.push(array);
        } else {
            // Find and add column from batch
            match batch.schema().index_of(field.name()) {
                Ok(col_idx) => {
                    columns.push(batch.column(col_idx).clone());
                }
                Err(_) => {
                    // Field not found in batch - this shouldn't happen for valid projections
                    // but we need to handle it gracefully
                    return Err(DataFusionError::Internal(format!(
                        "Field '{}' not found in batch schema",
                        field.name()
                    )));
                }
            }
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;

    fn create_test_parquet(dir: &Path, filename: &str, id_start: i64) -> PathBuf {
        use datafusion::parquet::arrow::ArrowWriter;

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

    #[test]
    fn test_discover_flat_directory() {
        let temp_dir = TempDir::new().unwrap();
        create_test_parquet(temp_dir.path(), "file1.parquet", 1);
        create_test_parquet(temp_dir.path(), "file2.parquet", 10);

        let config = ParquetTableConfig::new("test");
        let provider = ParquetTableProvider::try_new(temp_dir.path(), config).unwrap();

        assert_eq!(provider.file_count(), 2);
        assert_eq!(provider.schema().fields().len(), 2);
    }

    #[test]
    fn test_discover_partitioned_directory() {
        let temp_dir = TempDir::new().unwrap();

        // Create partitioned structure (all tables are partitioned by date)
        let part1 = temp_dir.path().join("date=2024-01-01");
        let part2 = temp_dir.path().join("date=2024-01-02");
        std::fs::create_dir_all(&part1).unwrap();
        std::fs::create_dir_all(&part2).unwrap();

        create_test_parquet(&part1, "data.parquet", 1);
        create_test_parquet(&part2, "data.parquet", 10);

        let config = ParquetTableConfig::new("test");
        let provider = ParquetTableProvider::try_new(temp_dir.path(), config).unwrap();

        assert_eq!(provider.file_count(), 2);
        // Schema should include date partition column
        assert!(provider.schema().field_with_name("date").is_ok());
    }

    #[tokio::test]
    async fn test_query_partitioned_table() {
        let temp_dir = TempDir::new().unwrap();

        let part1 = temp_dir.path().join("date=2024-01-01");
        std::fs::create_dir_all(&part1).unwrap();
        create_test_parquet(&part1, "data.parquet", 1);

        let config = ParquetTableConfig::new("test_table");
        let provider = ParquetTableProvider::try_new(temp_dir.path(), config).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT id, date FROM test").await.unwrap();
        let results = df.collect().await.unwrap();

        assert!(!results.is_empty());
        // Should have both id and date columns
        assert_eq!(results[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_query_with_projection() {
        let temp_dir = TempDir::new().unwrap();

        let part1 = temp_dir.path().join("date=2024-01-01");
        std::fs::create_dir_all(&part1).unwrap();
        create_test_parquet(&part1, "data.parquet", 1);

        let config = ParquetTableConfig::new("test_table");
        let provider = ParquetTableProvider::try_new(temp_dir.path(), config).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT name FROM test").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results[0].num_columns(), 1);
    }
}
