//! Dictionary Encoding for Cache Optimization
//!
//! Converts string columns (tags) to dictionary-encoded format to save memory.
//! Typical savings: 30-70% for low-cardinality string columns.

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, DictionaryArray, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use std::sync::Arc;

/// Minimum cardinality ratio to apply dictionary encoding
/// If unique_values / total_rows < THRESHOLD, apply encoding
const DICT_ENCODING_THRESHOLD: f64 = 0.5;

/// Minimum array size to consider for dictionary encoding (in rows)
const MIN_ARRAY_SIZE: usize = 100;

/// Apply dictionary encoding to tag columns in a RecordBatch
///
/// Detects string columns that are likely tags (low cardinality) and
/// converts them to dictionary-encoded format.
pub fn encode_batch(batch: &RecordBatch) -> Result<RecordBatch, datafusion::arrow::error::ArrowError> {
    let schema = batch.schema();
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let mut new_fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
    let mut encoded = false;

    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i);

        // Only encode string columns
        if matches!(field.data_type(), DataType::Utf8) && batch.num_rows() >= MIN_ARRAY_SIZE {
            // Check if this column has low cardinality
            if should_encode_column(column) {
                match encode_string_array(column) {
                    Ok(dict_array) => {
                        // Create new field with dictionary type
                        let dict_field = Field::new(
                            field.name(),
                            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                            field.is_nullable(),
                        );
                        new_fields.push(dict_field);
                        new_columns.push(dict_array);
                        encoded = true;
                        continue;
                    }
                    Err(_) => {
                        // Fall through to use original column
                    }
                }
            }
        }

        // Keep original column
        new_fields.push(field.as_ref().clone());
        new_columns.push(column.clone());
    }

    // If no columns were encoded, return original batch
    if !encoded {
        return Ok(batch.clone());
    }

    // Create new schema and batch with encoded columns
    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
}

/// Check if a string column should be dictionary-encoded
fn should_encode_column(array: &ArrayRef) -> bool {
    let string_array = match array.as_string_opt::<i32>() {
        Some(arr) => arr,
        None => return false,
    };

    // Calculate unique value ratio
    let total = string_array.len();
    if total < MIN_ARRAY_SIZE {
        return false;
    }

    // Sample-based cardinality check (more efficient than full scan)
    let sample_size = total.min(1000);
    let mut seen = std::collections::HashSet::new();

    for i in 0..sample_size {
        if string_array.is_valid(i) {
            if let Some(val) = string_array.value(i).get(0..32) {
                seen.insert(val);
            }
        }
    }

    let cardinality_ratio = seen.len() as f64 / sample_size as f64;
    cardinality_ratio < DICT_ENCODING_THRESHOLD
}

/// Encode a string array to dictionary format
fn encode_string_array(
    array: &ArrayRef,
) -> Result<ArrayRef, datafusion::arrow::error::ArrowError> {
    let string_array = array.as_string::<i32>();

    // Build dictionary array manually
    let mut builder = arrow::array::StringDictionaryBuilder::<Int32Type>::new();

    for i in 0..string_array.len() {
        if string_array.is_valid(i) {
            builder.append_value(string_array.value(i));
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Decode a dictionary-encoded batch back to regular format
///
/// This is useful if the downstream consumer doesn't support dictionary encoding.
pub fn decode_batch(batch: &RecordBatch) -> Result<RecordBatch, datafusion::arrow::error::ArrowError> {
    let schema = batch.schema();
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let mut new_fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
    let mut decoded = false;

    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i);

        // Check if this is a dictionary-encoded string column
        if let DataType::Dictionary(key_type, value_type) = field.data_type() {
            if matches!(key_type.as_ref(), DataType::Int32)
                && matches!(value_type.as_ref(), DataType::Utf8)
            {
                // Decode to regular string array using Arrow's cast
                let string_array = arrow::compute::cast(column, &DataType::Utf8)?;

                let string_field = Field::new(field.name(), DataType::Utf8, field.is_nullable());
                new_fields.push(string_field);
                new_columns.push(string_array);
                decoded = true;
                continue;
            }
        }

        // Keep original column
        new_fields.push(field.as_ref().clone());
        new_columns.push(column.clone());
    }

    // If no columns were decoded, return original batch
    if !decoded {
        return Ok(batch.clone());
    }

    // Create new schema and batch
    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
}

/// Estimate memory savings from dictionary encoding
///
/// Returns (original_size, encoded_size, savings_percent)
pub fn estimate_savings(batch: &RecordBatch) -> (usize, usize, f64) {
    let original_size: usize = batch
        .columns()
        .iter()
        .map(|col| col.get_buffer_memory_size())
        .sum();

    let encoded_batch = match encode_batch(batch) {
        Ok(b) => b,
        Err(_) => return (original_size, original_size, 0.0),
    };

    let encoded_size: usize = encoded_batch
        .columns()
        .iter()
        .map(|col| col.get_buffer_memory_size())
        .sum();

    let savings_percent = if original_size > 0 {
        ((original_size - encoded_size) as f64 / original_size as f64) * 100.0
    } else {
        0.0
    };

    (original_size, encoded_size, savings_percent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch_with_tags() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("service", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ]));

        let ids = Int64Array::from((0..10000).collect::<Vec<_>>());

        // Low cardinality with longer values - good for dictionary encoding
        let services: Vec<&str> = (0..10000)
            .map(|i| match i % 5 {
                0 => "web-frontend-service",
                1 => "api-gateway-service",
                2 => "database-primary-service",
                3 => "cache-redis-service",
                _ => "worker-background-service",
            })
            .collect();

        let regions: Vec<&str> = (0..10000)
            .map(|i| match i % 3 {
                0 => "us-west-2-production",
                1 => "us-east-1-production",
                _ => "eu-west-1-production",
            })
            .collect();

        let service_array = StringArray::from(services);
        let region_array = StringArray::from(regions);

        RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(service_array), Arc::new(region_array)],
        )
        .unwrap()
    }

    #[test]
    fn test_encode_batch() {
        let batch = create_test_batch_with_tags();

        let encoded = encode_batch(&batch).unwrap();

        // Check that string columns are now dictionary-encoded
        let schema = encoded.schema();
        assert!(matches!(
            schema.field(1).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert!(matches!(
            schema.field(2).data_type(),
            DataType::Dictionary(_, _)
        ));

        // Row count should be unchanged
        assert_eq!(encoded.num_rows(), batch.num_rows());
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let batch = create_test_batch_with_tags();

        let encoded = encode_batch(&batch).unwrap();
        let decoded = decode_batch(&encoded).unwrap();

        // Should match original schema
        assert_eq!(decoded.schema(), batch.schema());
        assert_eq!(decoded.num_rows(), batch.num_rows());
        assert_eq!(decoded.num_columns(), batch.num_columns());
    }

    #[test]
    fn test_estimate_savings() {
        let batch = create_test_batch_with_tags();

        let (original, encoded, savings) = estimate_savings(&batch);

        println!(
            "Original: {} bytes, Encoded: {} bytes, Savings: {:.1}%",
            original, encoded, savings
        );

        // Should have significant savings for low-cardinality strings
        assert!(savings > 30.0, "Expected >30% savings, got {:.1}%", savings);
        assert!(encoded < original);
    }
}
