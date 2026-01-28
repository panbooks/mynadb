//! JSON conversion utilities for Arrow RecordBatches

use arrow::array::*;
use arrow::datatypes::DataType;
use std::sync::Arc;

/// Convert multiple RecordBatches to JSON array
pub fn batches_to_json(batches: &[RecordBatch]) -> Result<Vec<serde_json::Value>, String> {
    let mut all_rows = Vec::new();
    for batch in batches {
        let rows = batch_to_json(batch)?;
        all_rows.extend(rows);
    }
    Ok(all_rows)
}

/// Convert a single RecordBatch to JSON objects
pub fn batch_to_json(batch: &RecordBatch) -> Result<Vec<serde_json::Value>, String> {
    let mut rows = Vec::new();
    let schema = batch.schema();
    let num_rows = batch.num_rows();

    for row_idx in 0..num_rows {
        let mut row_obj = serde_json::Map::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let value = array_value_to_json(column, row_idx)?;
            row_obj.insert(field.name().clone(), value);
        }

        rows.push(serde_json::Value::Object(row_obj));
    }

    Ok(rows)
}

/// Convert Arrow array value at given index to JSON
pub fn array_value_to_json(
    array: &Arc<dyn Array>,
    index: usize,
) -> Result<serde_json::Value, String> {
    use arrow::datatypes::UInt16Type;

    if array.is_null(index) {
        return Ok(serde_json::Value::Null);
    }

    match array.data_type() {
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(index);
            // Convert days since epoch to ISO date string
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            Ok(serde_json::json!(date.format("%Y-%m-%d").to_string()))
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::Timestamp(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(serde_json::json!(arr.value(index)))
        }
        DataType::Dictionary(key_type, _) => {
            match key_type.as_ref() {
                DataType::UInt16 => {
                    let dict_array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<UInt16Type>>()
                        .unwrap();
                    let keys = dict_array.keys();
                    let key = keys.value(index);
                    let values = dict_array.values();
                    array_value_to_json(values, key as usize)
                }
                _ => Ok(serde_json::json!(format!("<dict:{:?}>", array.data_type()))),
            }
        }
        other => Ok(serde_json::json!(format!("<{:?}>", other))),
    }
}
