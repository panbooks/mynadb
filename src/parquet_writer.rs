use crate::line_protocol::format_date32_as_partition;
use crate::storage::Storage;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

// File naming constants
const PARQUET_FILE_EXT: &str = "parquet";
const LEVEL_PREFIX: &str = "L";
const RAW_FILE_LEVEL: u32 = 0;

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

    /// Write batches to a partitioned directory structure
    /// Directory: catalog/schema/table/date=YYYY-MM-DD/
    pub async fn write_batches_partitioned(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        batches: Vec<RecordBatch>,
    ) -> Result<String, String> {
        if batches.is_empty() {
            return Err("No batches to write".to_string());
        }

        // Create directory structure
        let partition_path = Self::get_partition_path(catalog, schema, table, date);
        self.storage.create_dir(&partition_path).await?;

        // Generate filename with L0 prefix (Level 0 - raw files)
        let timestamp = chrono::Utc::now().timestamp_millis();
        let filename = Self::generate_filename(RAW_FILE_LEVEL, timestamp);
        let full_path = format!("{}/{}", partition_path, filename);

        // Merge schemas from all batches to create unified schema
        let merged_schema = Self::merge_schemas(&batches)?;
        let schema_ref = Arc::new(merged_schema);

        // Write to Parquet in memory buffer
        let mut buffer = Vec::new();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema_ref.clone(), Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        // Write all batches (may need to be adapted to merged schema)
        for batch in batches {
            if batch.schema() == schema_ref {
                writer
                    .write(&batch)
                    .map_err(|e| format!("Failed to write batch: {}", e))?;
            } else {
                let adapted = Self::adapt_batch_to_schema(&batch, &schema_ref)?;
                writer
                    .write(&adapted)
                    .map_err(|e| format!("Failed to write adapted batch: {}", e))?;
            }
        }

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        // Write buffer to storage
        self.storage.put(&full_path, Bytes::from(buffer)).await?;

        Ok(full_path)
    }

    /// Write batches synchronously (for backward compatibility with cache.rs)
    pub fn write_batches_partitioned_sync(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date: i32,
        batches: Vec<RecordBatch>,
    ) -> Result<String, String> {
        // Use tokio runtime to run async code
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(
                self.write_batches_partitioned(catalog, schema, table, date, batches),
            )
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

        // Maintain field order: timestamp, date, measurement, tags (sorted), fields (sorted)
        let mut fields = Vec::new();

        if let Some(f) = field_map.remove("timestamp") {
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
