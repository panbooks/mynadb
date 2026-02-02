//! Schema Operations for Compactor
//!
//! Handles schema alignment and type conversion for record batches.

use arrow::array::{
    new_null_array, Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

use super::Compactor;

impl Compactor {
    /// Align all batches to have a unified schema, handling type conflicts
    pub(crate) fn align_batch_schemas(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, String> {
        if batches.is_empty() {
            return Ok(batches);
        }

        // Fast path: check if all schemas are identical
        if batches.len() > 1 {
            let first_schema = batches[0].schema();
            let all_same = batches[1..]
                .iter()
                .all(|b| b.schema().as_ref() == first_schema.as_ref());

            if all_same {
                // All schemas are identical, no alignment needed
                return Ok(batches);
            }
        }

        // Step 1: Merge all schemas to create a unified schema
        let merged_schema = self.merge_schemas(&batches)?;

        // Step 2: Convert each batch to the merged schema
        let mut aligned_batches = Vec::new();
        for batch in batches {
            let aligned_batch = self.convert_batch_to_schema(&batch, &merged_schema)?;
            aligned_batches.push(aligned_batch);
        }

        Ok(aligned_batches)
    }

    /// Merge schemas from multiple batches, handling type conflicts
    pub(crate) fn merge_schemas(&self, batches: &[RecordBatch]) -> Result<Arc<Schema>, String> {
        let mut field_map: HashMap<String, DataType> = HashMap::new();
        let mut field_order: Vec<String> = Vec::new();

        // Collect all fields and their types
        for batch in batches {
            let schema = batch.schema();
            for field in schema.fields() {
                let field_name = field.name().clone();

                if let Some(existing_type) = field_map.get(&field_name) {
                    // Handle type conflicts
                    if existing_type != field.data_type() {
                        warn!(
                            "Schema conflict for field '{}': {:?} vs {:?}, using String as common type",
                            field_name, existing_type, field.data_type()
                        );
                        // Use String as the most flexible common type
                        field_map.insert(field_name.clone(), DataType::Utf8);
                    }
                } else {
                    field_map.insert(field_name.clone(), field.data_type().clone());
                    field_order.push(field_name);
                }
            }
        }

        // Build merged schema maintaining field order
        let fields: Vec<Field> = field_order
            .into_iter()
            .map(|name| {
                let data_type = field_map.get(&name).unwrap().clone();
                Field::new(name, data_type, true) // nullable=true for safety
            })
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Convert a batch to match a target schema, casting types as needed
    pub(crate) fn convert_batch_to_schema(
        &self,
        batch: &RecordBatch,
        target_schema: &Arc<Schema>,
    ) -> Result<RecordBatch, String> {
        // Fast path: if schemas are identical, return batch as-is
        if batch.schema().as_ref() == target_schema.as_ref() {
            return Ok(batch.clone());
        }

        let mut new_columns: Vec<ArrayRef> = Vec::new();

        for target_field in target_schema.fields() {
            let field_name = target_field.name();

            // Find matching column in source batch
            if let Some((col_idx, source_field)) = batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .find(|(_, f)| f.name() == field_name)
            {
                let source_array = batch.column(col_idx);

                // If types match, use as-is
                if source_field.data_type() == target_field.data_type() {
                    new_columns.push(source_array.clone());
                } else {
                    // Try to cast to target type
                    match cast(source_array, target_field.data_type()) {
                        Ok(casted_array) => {
                            new_columns.push(casted_array);
                        }
                        Err(_) => {
                            // Cast failed, convert to string as last resort
                            warn!(
                                "Failed to cast '{}' from {:?} to {:?}, converting to string",
                                field_name,
                                source_field.data_type(),
                                target_field.data_type()
                            );
                            let string_array = self.convert_array_to_string(source_array)?;
                            new_columns.push(Arc::new(string_array));
                        }
                    }
                }
            } else {
                // Field doesn't exist in source batch, add null array
                let null_array = new_null_array(target_field.data_type(), batch.num_rows());
                new_columns.push(null_array);
            }
        }

        RecordBatch::try_new(target_schema.clone(), new_columns)
            .map_err(|e| format!("Failed to create aligned batch: {}", e))
    }

    /// Convert any array type to StringArray
    pub(crate) fn convert_array_to_string(&self, array: &ArrayRef) -> Result<StringArray, String> {
        let num_rows = array.len();
        let mut string_values: Vec<Option<String>> = Vec::with_capacity(num_rows);

        match array.data_type() {
            DataType::Int64 => {
                let int_array = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("Failed to downcast to Int64Array")?;
                for i in 0..num_rows {
                    string_values.push(if int_array.is_null(i) {
                        None
                    } else {
                        Some(int_array.value(i).to_string())
                    });
                }
            }
            DataType::Float64 => {
                let float_array = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or("Failed to downcast to Float64Array")?;
                for i in 0..num_rows {
                    string_values.push(if float_array.is_null(i) {
                        None
                    } else {
                        Some(float_array.value(i).to_string())
                    });
                }
            }
            DataType::Boolean => {
                let bool_array = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or("Failed to downcast to BooleanArray")?;
                for i in 0..num_rows {
                    string_values.push(if bool_array.is_null(i) {
                        None
                    } else {
                        Some(bool_array.value(i).to_string())
                    });
                }
            }
            DataType::Utf8 => {
                let str_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or("Failed to downcast to StringArray")?;
                for i in 0..num_rows {
                    string_values.push(if str_array.is_null(i) {
                        None
                    } else {
                        Some(str_array.value(i).to_string())
                    });
                }
            }
            _ => {
                // For unsupported types, use debug format
                for i in 0..num_rows {
                    string_values.push(if array.is_null(i) {
                        None
                    } else {
                        Some(format!("{:?}", array))
                    });
                }
            }
        }

        Ok(StringArray::from(string_values))
    }
}
