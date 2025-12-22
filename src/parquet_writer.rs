use crate::line_protocol::{FieldValue, Point};
use arrow::array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::path::Path;
use std::sync::Arc;

pub struct ParquetWriter {
    data_dir: String,
}

impl ParquetWriter {
    pub fn new(data_dir: &str) -> Self {
        ParquetWriter {
            data_dir: data_dir.to_string(),
        }
    }

    pub fn write_batches(
        &self,
        db_name: &str,
        measurement: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<String, String> {
        if batches.is_empty() {
            return Err("No batches to write".to_string());
        }

        // Create directory structure
        let db_path = format!("{}/{}", self.data_dir, db_name);
        create_dir_all(&db_path).map_err(|e| format!("Failed to create directory: {}", e))?;

        // Generate filename with timestamp
        let filename = format!(
            "{}/{}_{}.parquet",
            db_path,
            measurement,
            chrono::Utc::now().timestamp_millis()
        );

        // Get schema from first batch
        let schema = batches[0].schema();

        // Write to Parquet
        let file = File::create(&filename).map_err(|e| format!("Failed to create file: {}", e))?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        // Write all batches
        for batch in batches {
            writer
                .write(&batch)
                .map_err(|e| format!("Failed to write batch: {}", e))?;
        }

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        Ok(filename)
    }

    pub fn write_points(
        &self,
        db_name: &str,
        measurement: &str,
        points: Vec<Point>,
    ) -> Result<String, String> {
        if points.is_empty() {
            return Err("No points to write".to_string());
        }

        // Create directory structure
        let db_path = format!("{}/{}", self.data_dir, db_name);
        create_dir_all(&db_path).map_err(|e| format!("Failed to create directory: {}", e))?;

        // Generate filename with timestamp
        let filename = format!(
            "{}/{}_{}.parquet",
            db_path,
            measurement,
            chrono::Utc::now().timestamp_millis()
        );

        // Build schema dynamically from first point
        let schema = self.build_schema(&points[0])?;
        let schema_ref = Arc::new(schema);

        // Create RecordBatch
        let batch = self.points_to_record_batch(&points, schema_ref.clone())?;

        // Write to Parquet
        let file = File::create(&filename).map_err(|e| format!("Failed to create file: {}", e))?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema_ref, Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        writer
            .write(&batch)
            .map_err(|e| format!("Failed to write batch: {}", e))?;
        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        Ok(filename)
    }

    fn build_schema(&self, point: &Point) -> Result<Schema, String> {
        let mut fields = Vec::new();

        // Timestamp field (always first)
        fields.push(Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));

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