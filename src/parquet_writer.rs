use crate::line_protocol::{FieldValue, Point, format_date32_as_partition};
use arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// File naming constants
const PARQUET_FILE_EXT: &str = "parquet";
const LEVEL_PREFIX: &str = "L";
const RAW_FILE_LEVEL: u32 = 0;

pub struct ParquetWriter {
    data_dir: String,
}

impl ParquetWriter {
    pub fn new(data_dir: &str) -> Self {
        ParquetWriter {
            data_dir: data_dir.to_string(),
        }
    }

    /// Get table directory path: data_dir/catalog/schema/table/
    fn get_table_path(&self, catalog: &str, schema: &str, table: &str) -> PathBuf {
        Path::new(&self.data_dir).join(catalog).join(schema).join(table)
    }

    /// Get partition directory path: data_dir/catalog/schema/table/date=YYYY-MM-DD/
    fn get_partition_path(&self, catalog: &str, schema: &str, table: &str, date: i32) -> PathBuf {
        self.get_table_path(catalog, schema, table)
            .join(format_date32_as_partition(date))
    }

    /// Generate a leveled parquet filename
    fn generate_filename(level: u32, timestamp: i64) -> String {
        format!("{}{}_{}.{}", LEVEL_PREFIX, level, timestamp, PARQUET_FILE_EXT)
    }

    /// Write batches to a partitioned directory structure
    /// Directory: data_dir/catalog/schema/table/date=YYYY-MM-DD/
    pub fn write_batches_partitioned(
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

        // Create directory structure: data/catalog/schema/table/date=YYYY-MM-DD/
        let partition_path = self.get_partition_path(catalog, schema, table, date);
        create_dir_all(&partition_path)
            .map_err(|e| format!("Failed to create partition directory: {}", e))?;

        // Generate filename with L0 prefix (Level 0 - raw files)
        let timestamp = chrono::Utc::now().timestamp_millis();
        let filename = partition_path.join(Self::generate_filename(RAW_FILE_LEVEL, timestamp));

        // Merge schemas from all batches to create unified schema
        let merged_schema = Self::merge_schemas(&batches)?;
        let schema_ref = Arc::new(merged_schema);

        // Write to Parquet
        let file = File::create(&filename).map_err(|e| format!("Failed to create file: {}", e))?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema_ref.clone(), Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        // Write all batches (may need to be adapted to merged schema)
        for batch in batches {
            // If batch schema matches merged schema, write directly
            if batch.schema() == schema_ref {
                writer
                    .write(&batch)
                    .map_err(|e| format!("Failed to write batch: {}", e))?;
            } else {
                // Adapt batch to merged schema
                let adapted = Self::adapt_batch_to_schema(&batch, &schema_ref)?;
                writer
                    .write(&adapted)
                    .map_err(|e| format!("Failed to write adapted batch: {}", e))?;
            }
        }

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        Ok(filename.to_string_lossy().to_string())
    }

    /// Legacy method: write batches without date partitioning (for backward compatibility)
    #[allow(dead_code)]
    pub fn write_batches(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<String, String> {
        if batches.is_empty() {
            return Err("No batches to write".to_string());
        }

        // Create directory structure: data/catalog/schema/table/
        let table_path = self.get_table_path(catalog, schema, table);
        create_dir_all(&table_path)
            .map_err(|e| format!("Failed to create table directory: {}", e))?;

        // Generate filename with L0 prefix (Level 0 - raw files)
        let timestamp = chrono::Utc::now().timestamp_millis();
        let filename = table_path.join(Self::generate_filename(RAW_FILE_LEVEL, timestamp));

        // Merge schemas from all batches to create unified schema
        let merged_schema = Self::merge_schemas(&batches)?;
        let schema_ref = Arc::new(merged_schema);

        // Write to Parquet
        let file = File::create(&filename).map_err(|e| format!("Failed to create file: {}", e))?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema_ref.clone(), Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        // Write all batches (may need to be adapted to merged schema)
        for batch in batches {
            // If batch schema matches merged schema, write directly
            if batch.schema() == schema_ref {
                writer
                    .write(&batch)
                    .map_err(|e| format!("Failed to write batch: {}", e))?;
            } else {
                // Adapt batch to merged schema
                let adapted = Self::adapt_batch_to_schema(&batch, &schema_ref)?;
                writer
                    .write(&adapted)
                    .map_err(|e| format!("Failed to write adapted batch: {}", e))?;
            }
        }

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        Ok(filename.to_string_lossy().to_string())
    }

    /// Merge schemas from multiple batches, keeping all unique fields
    fn merge_schemas(batches: &[RecordBatch]) -> Result<Schema, String> {
        use std::collections::HashMap;

        let mut field_map: HashMap<String, Field> = HashMap::new();

        for batch in batches {
            for field in batch.schema().fields() {
                let name = field.name().clone();
                if let Some(existing) = field_map.get(&name) {
                    // Check type compatibility
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

        // Add in priority order
        if let Some(f) = field_map.remove("timestamp") {
            fields.push(f);
        }
        if let Some(f) = field_map.remove("date") {
            fields.push(f);
        }
        if let Some(f) = field_map.remove("measurement") {
            fields.push(f);
        }

        // Add remaining fields in sorted order
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

            // Check if field exists in source batch
            if let Some(column) = batch.column_by_name(field_name) {
                arrays.push(column.clone());
            } else {
                // Field doesn't exist, create null array
                let null_array = new_null_array(target_field.data_type(), batch.num_rows());
                arrays.push(null_array);
            }
        }

        RecordBatch::try_new(target_schema.clone(), arrays)
            .map_err(|e| format!("Failed to create adapted batch: {}", e))
    }

    fn build_schema(&self, point: &Point) -> Result<Schema, String> {
        let mut fields = Vec::new();

        // Timestamp field (always first)
        fields.push(Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));

        // Date field (Date32)
        fields.push(Field::new("date", DataType::Date32, false));

        // Measurement field
        fields.push(Field::new("measurement", DataType::Utf8, false));

        // Tag fields
        let mut tag_keys: Vec<_> = point.tags.keys().collect();
        tag_keys.sort();
        for key in tag_keys {
            fields.push(Field::new(key, DataType::Utf8, true));
        }

        // Field columns
        let mut field_keys: Vec<_> = point.fields.keys().collect();
        field_keys.sort();
        for key in field_keys {
            let data_type = match point.fields.get(key.as_str()).unwrap() {
                FieldValue::Float(_) => DataType::Float64,
                FieldValue::Integer(_) => DataType::Int64,
                FieldValue::String(_) => DataType::Utf8,
                FieldValue::Boolean(_) => DataType::Boolean,
            };
            fields.push(Field::new(key, data_type, true));
        }

        Ok(Schema::new(fields))
    }

    #[allow(dead_code)]
    fn points_to_record_batch(
        &self,
        points: &[Point],
        schema: Arc<Schema>,
    ) -> Result<RecordBatch, String> {
        let mut arrays: Vec<ArrayRef> = Vec::new();

        // Build timestamp array
        let timestamps: Vec<i64> = points
            .iter()
            .map(|p| {
                p.timestamp
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0))
            })
            .collect();
        arrays.push(Arc::new(TimestampNanosecondArray::from(timestamps)));

        // Build date array (Date32)
        let dates: Vec<i32> = points.iter().map(|p| p.date).collect();
        arrays.push(Arc::new(Date32Array::from(dates)));

        // Build measurement array
        let measurements: Vec<&str> = points.iter().map(|p| p.measurement.as_str()).collect();
        arrays.push(Arc::new(StringArray::from(measurements)));

        // Build tag arrays
        let first_point = &points[0];
        let mut tag_keys: Vec<_> = first_point.tags.keys().collect();
        tag_keys.sort();

        for key in tag_keys {
            let values: Vec<Option<&str>> = points
                .iter()
                .map(|p| p.tags.get(key.as_str()).map(|s| s.as_str()))
                .collect();
            arrays.push(Arc::new(StringArray::from(values)));
        }

        // Build field arrays
        let mut field_keys: Vec<_> = first_point.fields.keys().collect();
        field_keys.sort();

        for key in field_keys {
            let field_type = first_point.fields.get(key.as_str()).unwrap();
            match field_type {
                FieldValue::Float(_) => {
                    let values: Vec<Option<f64>> = points
                        .iter()
                        .map(|p| {
                            p.fields.get(key.as_str()).and_then(|v| match v {
                                FieldValue::Float(f) => Some(*f),
                                _ => None,
                            })
                        })
                        .collect();
                    arrays.push(Arc::new(Float64Array::from(values)));
                }
                FieldValue::Integer(_) => {
                    let values: Vec<Option<i64>> = points
                        .iter()
                        .map(|p| {
                            p.fields.get(key.as_str()).and_then(|v| match v {
                                FieldValue::Integer(i) => Some(*i),
                                _ => None,
                            })
                        })
                        .collect();
                    arrays.push(Arc::new(Int64Array::from(values)));
                }
                FieldValue::String(_) => {
                    let values: Vec<Option<&str>> = points
                        .iter()
                        .map(|p| {
                            p.fields.get(key.as_str()).and_then(|v| match v {
                                FieldValue::String(s) => Some(s.as_str()),
                                _ => None,
                            })
                        })
                        .collect();
                    arrays.push(Arc::new(StringArray::from(values)));
                }
                FieldValue::Boolean(_) => {
                    let values: Vec<Option<bool>> = points
                        .iter()
                        .map(|p| {
                            p.fields.get(key.as_str()).and_then(|v| match v {
                                FieldValue::Boolean(b) => Some(*b),
                                _ => None,
                            })
                        })
                        .collect();
                    arrays.push(Arc::new(
                        arrow::array::BooleanArray::from(values),
                    ));
                }
            }
        }

        RecordBatch::try_new(schema, arrays).map_err(|e| format!("Failed to create batch: {}", e))
    }
}
