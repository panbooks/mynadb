use crate::line_protocol::format_date32_as_partition;
use crate::storage::storage::Storage;
use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, lexsort_to_indices, sort_to_indices, take, SortColumn};
use arrow::datatypes::{Field, Schema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tracing::{debug, warn};

// File naming constants
const PARQUET_FILE_EXT: &str = "parquet";
const LEVEL_PREFIX: &str = "L";
const RAW_FILE_LEVEL: u32 = 0;

/// Result of a file write operation, containing metadata for registration
#[derive(Debug, Clone)]
pub struct FileWriteResult {
    /// Full path to the written file
    pub path: String,
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Partition key (e.g., "2024-01-15")
    pub partition_key: String,
    /// File size in bytes
    pub size_bytes: u64,
    /// Number of rows written
    pub row_count: u64,
    /// Compaction level (0 = raw)
    pub level: u8,
}

// Parquet optimization constants
// 500K rows per row group - optimal balance between:
// - Parallelism (more row groups = more parallel reads)
// - Statistics granularity (better predicate pruning)
// - Memory usage (smaller groups = less memory per read)
// - Compression ratio (larger groups = slightly better compression)
const ROW_GROUP_SIZE: usize = 500_000;
// Data page size - 1MB for good predicate pushdown performance
const DATA_PAGE_SIZE: usize = 1024 * 1024;
// Dictionary page size limit
const DICTIONARY_PAGE_SIZE: usize = 1024 * 1024;
// Bloom filter false positive rate (1%)
const BLOOM_FILTER_FPP: f64 = 0.01;
// Expected distinct values for bloom filter sizing
const BLOOM_FILTER_NDV: u64 = 10_000;

pub struct ParquetWriter {
    storage: Arc<Storage>,
}

impl ParquetWriter {
    pub fn new(storage: Arc<Storage>) -> Self {
        ParquetWriter { storage }
    }

    /// Create from data directory (local storage only, for backward compatibility)
    pub fn new_local(data_dir: &str) -> Result<Self, String> {
        let storage = Storage::new_local(data_dir)?;
        Ok(ParquetWriter {
            storage: Arc::new(storage),
        })
    }

    /// Get table path: catalog/schema/table/
    fn get_table_path(catalog: &str, schema: &str, table: &str) -> String {
        format!("{}/{}/{}", catalog, schema, table)
    }

    /// Get partition path: catalog/schema/table/date=YYYY-MM-DD/
    fn get_partition_path(catalog: &str, schema: &str, table: &str, date: i32) -> String {
        format!(
            "{}/{}",
            Self::get_table_path(catalog, schema, table),
            format_date32_as_partition(date)
        )
    }

    /// Generate a leveled parquet filename
    fn generate_filename(level: u32, timestamp: i64) -> String {
        format!(
            "{}{}_{}.{}",
            LEVEL_PREFIX, level, timestamp, PARQUET_FILE_EXT
        )
    }

    /// Sort a RecordBatch by time column only (ascending) - legacy method
    fn sort_batch_by_time(batch: &RecordBatch) -> Result<RecordBatch, String> {
        Self::sort_batch_by_columns(batch, &["time".to_string()])
    }

    /// Sort a RecordBatch by multiple columns (ascending)
    /// Columns are sorted in the order provided (first column is primary sort key)
    fn sort_batch_by_columns(
        batch: &RecordBatch,
        sort_columns: &[String],
    ) -> Result<RecordBatch, String> {
        if sort_columns.is_empty() {
            return Err("No sort columns provided".to_string());
        }

        // Build SortColumn specifications for lexsort
        let mut sort_cols: Vec<SortColumn> = Vec::with_capacity(sort_columns.len());

        for col_name in sort_columns {
            match batch.schema().index_of(col_name) {
                Ok(idx) => {
                    sort_cols.push(SortColumn {
                        values: batch.column(idx).clone(),
                        options: None, // Default: ascending, nulls last
                    });
                }
                Err(_) => {
                    // Column not found - skip (may be optional tag)
                    warn!("Sort column '{}' not found in schema, skipping", col_name);
                }
            }
        }

        if sort_cols.is_empty() {
            return Err("No valid sort columns found in schema".to_string());
        }

        // Perform lexicographic sort
        let indices = lexsort_to_indices(&sort_cols, None)
            .map_err(|e| format!("Failed to lexsort: {}", e))?;

        // Reorder all columns by the sort indices
        let sorted_columns: Result<Vec<ArrayRef>, String> = batch
            .columns()
            .iter()
            .map(|col| {
                take(col.as_ref(), &indices, None)
                    .map_err(|e| format!("Failed to reorder column: {}", e))
            })
            .collect();

        RecordBatch::try_new(batch.schema(), sorted_columns?)
            .map_err(|e| format!("Failed to create sorted batch: {}", e))
    }

    /// Build optimal sort order: low cardinality tags first, then time
    /// This enables efficient partition pruning and data locality
    fn build_sort_order(schema: &Schema, tag_order_by_cardinality: &[String]) -> Vec<String> {
        let mut sort_order = Vec::new();

        // Add tags in cardinality order (low to high)
        for tag in tag_order_by_cardinality {
            // Verify tag exists in schema
            if schema.index_of(tag).is_ok() {
                sort_order.push(tag.clone());
            }
        }

        // Always end with time for time-range locality
        sort_order.push("time".to_string());

        sort_order
    }

    /// Create optimized WriterProperties with Bloom filters, compression, and tuned row groups
    fn create_optimized_writer_props(schema: &Schema) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            // ===== Row Group Tuning =====
            // 500K rows per row group - optimal for parallel reads and statistics granularity
            .set_max_row_group_size(ROW_GROUP_SIZE)
            // ===== Page Size Tuning =====
            // 1MB data pages for good predicate pushdown performance
            .set_data_page_size_limit(DATA_PAGE_SIZE)
            // Dictionary page size limit
            .set_dictionary_page_size_limit(DICTIONARY_PAGE_SIZE)
            // ===== Compression =====
            // ZSTD level 3 - good balance of speed and compression ratio
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()))
            // ===== Dictionary Encoding =====
            // Enable dictionary encoding for better compression of repeated values (tags)
            .set_dictionary_enabled(true)
            // ===== Statistics =====
            // Enable page-level statistics for fine-grained predicate pruning
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            // ===== Write Batch Size =====
            // Larger write batch for better I/O efficiency
            .set_write_batch_size(10000);

        // ===== Bloom Filters for String Columns =====
        // Enable Bloom filters for string columns (tags) to speed up equality predicates
        for field in schema.fields() {
            if field.data_type() == &arrow::datatypes::DataType::Utf8 {
                let col_path: parquet::schema::types::ColumnPath = field.name().clone().into();
                builder = builder
                    .set_column_bloom_filter_enabled(col_path.clone(), true)
                    .set_column_bloom_filter_fpp(col_path.clone(), BLOOM_FILTER_FPP)
                    .set_column_bloom_filter_ndv(col_path, BLOOM_FILTER_NDV);
            }
        }

        builder.build()
    }

    /// Write batches to a partitioned directory structure
    /// Directory: catalog/schema/table/date=YYYY-MM-DD/
    /// Batches are merged, sorted by timestamp, and written with optimized settings
    /// Returns detailed file write result for metadata registration
    pub async fn write_batches_partitioned(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        batches: Vec<RecordBatch>,
    ) -> Result<FileWriteResult, String> {
        // Default: sort by time only (backward compatible)
        self.write_batches_partitioned_with_sort(catalog, schema, table, date, batches, &[])
            .await
    }

    /// Write batches with cardinality-based sort ordering
    /// tag_order_by_cardinality: tags sorted from low to high cardinality
    /// Sort order will be: [low_cardinality_tags...] -> time
    pub async fn write_batches_partitioned_with_sort(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        batches: Vec<RecordBatch>,
        tag_order_by_cardinality: &[String],
    ) -> Result<FileWriteResult, String> {
        if batches.is_empty() {
            return Err("No batches to write".to_string());
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Create directory structure
        let partition_path = Self::get_partition_path(catalog, schema, table, date);
        let partition_key = format_date32_as_partition(date);
        self.storage.create_dir(&partition_path).await?;

        // Generate filename with L0 prefix (Level 0 - raw files)
        let timestamp = chrono::Utc::now().timestamp_millis();
        let filename = Self::generate_filename(RAW_FILE_LEVEL, timestamp);
        let full_path = format!("{}/{}", partition_path, filename);

        // Merge schemas from all batches to create unified schema
        let merged_schema = Self::merge_schemas(&batches)?;
        let schema_ref = Arc::new(merged_schema);

        // Adapt all batches to merged schema
        let adapted_batches: Result<Vec<RecordBatch>, String> = batches
            .into_iter()
            .map(|batch| {
                if batch.schema() == schema_ref {
                    Ok(batch)
                } else {
                    Self::adapt_batch_to_schema(&batch, &schema_ref)
                }
            })
            .collect();
        let adapted_batches = adapted_batches?;

        // Merge all batches into one
        let merged_batch = concat_batches(&schema_ref, &adapted_batches)
            .map_err(|e| format!("Failed to merge batches: {}", e))?;

        // Build optimal sort order based on cardinality
        let sort_order = Self::build_sort_order(&schema_ref, tag_order_by_cardinality);
        let sorted_batch = Self::sort_batch_by_columns(&merged_batch, &sort_order)?;

        let sort_desc = if tag_order_by_cardinality.is_empty() {
            "time".to_string()
        } else {
            format!("{} -> time", tag_order_by_cardinality.join(" -> "))
        };
        debug!(
            "Writing {} rows to {} (sorted by: {})",
            total_rows, full_path, sort_desc
        );

        // Write to Parquet with optimized settings
        let mut buffer = Vec::new();
        let props = Self::create_optimized_writer_props(&schema_ref);
        let mut writer = ArrowWriter::try_new(&mut buffer, schema_ref.clone(), Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        writer
            .write(&sorted_batch)
            .map_err(|e| format!("Failed to write batch: {}", e))?;

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        let size_bytes = buffer.len() as u64;

        // Write buffer to storage
        self.storage.put(&full_path, Bytes::from(buffer)).await?;

        Ok(FileWriteResult {
            path: full_path,
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            partition_key,
            size_bytes,
            row_count: total_rows as u64,
            level: RAW_FILE_LEVEL as u8,
        })
    }

    /// Write batches synchronously (for backward compatibility with cache.rs)
    /// Returns detailed file write result for metadata registration
    pub fn write_batches_partitioned_sync(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        batches: Vec<RecordBatch>,
    ) -> Result<FileWriteResult, String> {
        self.write_batches_partitioned_with_sort_sync(catalog, schema, table, date, batches, &[])
    }

    /// Write batches synchronously with cardinality-based sort ordering
    pub fn write_batches_partitioned_with_sort_sync(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        batches: Vec<RecordBatch>,
        tag_order_by_cardinality: &[String],
    ) -> Result<FileWriteResult, String> {
        // Use tokio runtime to run async code
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.write_batches_partitioned_with_sort(
                catalog,
                schema,
                table,
                date,
                batches,
                tag_order_by_cardinality,
            ))
        })
    }

    /// Merge schemas from multiple batches, keeping all unique fields
    fn merge_schemas(batches: &[RecordBatch]) -> Result<Schema, String> {
        use std::collections::HashMap;

        let mut field_map: HashMap<String, Field> = HashMap::new();

        for batch in batches {
            for field in batch.schema().fields() {
                let name = field.name().clone();
                if let Some(existing) = field_map.get(&name) {
                    if existing.data_type() != field.data_type() {
                        return Err(format!(
                            "Incompatible types for field '{}': {:?} vs {:?}",
                            name,
                            existing.data_type(),
                            field.data_type()
                        ));
                    }
                } else {
                    field_map.insert(name, field.as_ref().clone());
                }
            }
        }

        // Maintain field order: time, date, measurement, tags (sorted), fields (sorted)
        let mut fields = Vec::new();

        if let Some(f) = field_map.remove("time") {
            fields.push(f);
        }
        if let Some(f) = field_map.remove("date") {
            fields.push(f);
        }
        if let Some(f) = field_map.remove("measurement") {
            fields.push(f);
        }

        let mut remaining: Vec<_> = field_map.into_iter().collect();
        remaining.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, field) in remaining {
            fields.push(field);
        }

        Ok(Schema::new(fields))
    }

    /// Adapt a batch to match a target schema
    fn adapt_batch_to_schema(
        batch: &RecordBatch,
        target_schema: &Arc<Schema>,
    ) -> Result<RecordBatch, String> {
        use arrow::array::new_null_array;

        let mut arrays: Vec<ArrayRef> = Vec::new();

        for target_field in target_schema.fields() {
            let field_name = target_field.name();

            if let Some(column) = batch.column_by_name(field_name) {
                arrays.push(column.clone());
            } else {
                let null_array = new_null_array(target_field.data_type(), batch.num_rows());
                arrays.push(null_array);
            }
        }

        RecordBatch::try_new(target_schema.clone(), arrays)
            .map_err(|e| format!("Failed to create adapted batch: {}", e))
    }

    /// Get the underlying storage
    pub fn storage(&self) -> &Arc<Storage> {
        &self.storage
    }
}
