//! Fill Processor for Gap-Filling in Time Series Results
//!
//! Applies fill strategies to query results after GROUP BY time() queries.
//! This handles missing time buckets by inserting rows according to the
//! specified fill option.
//!
//! Fill options:
//! - `fill(null)` - Insert rows with null values for missing buckets
//! - `fill(none)` - No filling, omit missing buckets (default)
//! - `fill(previous)` - Forward-fill from last known value
//! - `fill(linear)` - Linear interpolation between adjacent values
//! - `fill(<number>)` - Fill with a constant value

use crate::query::time_interval::FillOption;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// Fill processor for time series query results
pub struct FillProcessor {
    /// Time bucket interval in nanoseconds
    interval_nanos: i64,
    /// Fill option to apply
    fill_option: FillOption,
    /// Index of the time column in the result
    time_column_idx: usize,
    /// Indices of columns to group by (excluding time)
    group_column_indices: Vec<usize>,
}

impl FillProcessor {
    /// Create a new fill processor
    pub fn new(
        interval_nanos: i64,
        fill_option: FillOption,
        time_column_idx: usize,
        group_column_indices: Vec<usize>,
    ) -> Self {
        Self {
            interval_nanos,
            fill_option,
            time_column_idx,
            group_column_indices,
        }
    }

    /// Apply fill processing to query results
    ///
    /// Returns the filled results as a vector of RecordBatches
    pub fn apply(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, FillError> {
        // If fill option is None, return as-is
        if matches!(self.fill_option, FillOption::None) {
            return Ok(batches);
        }

        if batches.is_empty() {
            return Ok(batches);
        }

        // Combine all batches into one for processing
        let schema = batches[0].schema();
        let combined = self.combine_batches(&batches)?;

        if combined.num_rows() == 0 {
            return Ok(batches);
        }

        // Get time range from the data
        let time_col = combined
            .column(self.time_column_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(FillError::InvalidTimeColumn)?;

        let (min_time, max_time) = self.get_time_range(time_col)?;

        // If we have group columns, process each group separately
        if !self.group_column_indices.is_empty() {
            self.fill_grouped(&combined, min_time, max_time, schema)
        } else {
            self.fill_single_series(&combined, min_time, max_time, schema)
        }
    }

    /// Combine multiple batches into one
    fn combine_batches(&self, batches: &[RecordBatch]) -> Result<RecordBatch, FillError> {
        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }

        datafusion::arrow::compute::concat_batches(&batches[0].schema(), batches)
            .map_err(|e| FillError::ArrowError(e.to_string()))
    }

    /// Get min and max time from the time column
    fn get_time_range(&self, time_col: &Int64Array) -> Result<(i64, i64), FillError> {
        let mut min_time = i64::MAX;
        let mut max_time = i64::MIN;

        for i in 0..time_col.len() {
            if !time_col.is_null(i) {
                let t = time_col.value(i);
                min_time = min_time.min(t);
                max_time = max_time.max(t);
            }
        }

        if min_time == i64::MAX {
            return Err(FillError::NoValidTimeValues);
        }

        Ok((min_time, max_time))
    }

    /// Fill a single time series (no grouping)
    fn fill_single_series(
        &self,
        batch: &RecordBatch,
        min_time: i64,
        max_time: i64,
        schema: Arc<Schema>,
    ) -> Result<Vec<RecordBatch>, FillError> {
        // Build expected time buckets
        let expected_times = self.generate_time_buckets(min_time, max_time);

        // Build a map of existing data by time
        let time_col = batch
            .column(self.time_column_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(FillError::InvalidTimeColumn)?;

        let mut existing_data: HashMap<i64, usize> = HashMap::new();
        for i in 0..time_col.len() {
            if !time_col.is_null(i) {
                existing_data.insert(time_col.value(i), i);
            }
        }

        // Build filled arrays
        let mut filled_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            let filled_col = self.fill_column(
                col,
                &expected_times,
                &existing_data,
                col_idx == self.time_column_idx,
            )?;
            filled_columns.push(filled_col);
        }

        let filled_batch = RecordBatch::try_new(schema, filled_columns)
            .map_err(|e| FillError::ArrowError(e.to_string()))?;

        Ok(vec![filled_batch])
    }

    /// Fill grouped time series
    fn fill_grouped(
        &self,
        batch: &RecordBatch,
        min_time: i64,
        max_time: i64,
        schema: Arc<Schema>,
    ) -> Result<Vec<RecordBatch>, FillError> {
        // For simplicity, we'll handle grouped data by processing each unique
        // combination of group values separately

        // Extract group key values
        let groups = self.extract_groups(batch)?;

        let mut all_results: Vec<RecordBatch> = Vec::new();
        let expected_times = self.generate_time_buckets(min_time, max_time);

        for (group_key, row_indices) in groups {
            // Filter batch to this group
            let group_batch = self.filter_to_rows(batch, &row_indices)?;

            // Fill this group
            let time_col = group_batch
                .column(self.time_column_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(FillError::InvalidTimeColumn)?;

            let mut existing_data: HashMap<i64, usize> = HashMap::new();
            for i in 0..time_col.len() {
                if !time_col.is_null(i) {
                    existing_data.insert(time_col.value(i), i);
                }
            }

            // Build filled arrays for this group
            let mut filled_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

            for col_idx in 0..batch.num_columns() {
                let col = group_batch.column(col_idx);
                let is_group_col = self.group_column_indices.contains(&col_idx);
                let is_time_col = col_idx == self.time_column_idx;

                let filled_col = if is_group_col {
                    // For group columns, repeat the group value
                    self.fill_constant_column(col, expected_times.len(), &group_key, col_idx)?
                } else {
                    self.fill_column(col, &expected_times, &existing_data, is_time_col)?
                };
                filled_columns.push(filled_col);
            }

            let filled_batch = RecordBatch::try_new(schema.clone(), filled_columns)
                .map_err(|e| FillError::ArrowError(e.to_string()))?;

            all_results.push(filled_batch);
        }

        // Combine all group results
        if all_results.is_empty() {
            return Ok(vec![]);
        }

        let combined = self.combine_batches(&all_results)?;
        Ok(vec![combined])
    }

    /// Generate expected time buckets between min and max
    fn generate_time_buckets(&self, min_time: i64, max_time: i64) -> Vec<i64> {
        let mut times = Vec::new();
        let mut t = min_time;
        while t <= max_time {
            times.push(t);
            t += self.interval_nanos;
        }
        times
    }

    /// Extract groups and their row indices
    fn extract_groups(
        &self,
        batch: &RecordBatch,
    ) -> Result<HashMap<Vec<String>, Vec<usize>>, FillError> {
        let mut groups: HashMap<Vec<String>, Vec<usize>> = HashMap::new();

        for row_idx in 0..batch.num_rows() {
            let mut key = Vec::new();
            for &col_idx in &self.group_column_indices {
                let col = batch.column(col_idx);
                let val = self.get_string_value(col, row_idx)?;
                key.push(val);
            }
            groups.entry(key).or_default().push(row_idx);
        }

        Ok(groups)
    }

    /// Get string representation of a value for grouping
    fn get_string_value(&self, col: &ArrayRef, row_idx: usize) -> Result<String, FillError> {
        if col.is_null(row_idx) {
            return Ok("NULL".to_string());
        }

        match col.data_type() {
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(arr.value(row_idx).to_string())
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(arr.value(row_idx).to_string())
            }
            DataType::UInt64 => {
                let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(arr.value(row_idx).to_string())
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(arr.value(row_idx).to_string())
            }
            DataType::Boolean => {
                let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(arr.value(row_idx).to_string())
            }
            _ => Ok(format!("{:?}", col.data_type())),
        }
    }

    /// Filter batch to specific row indices
    fn filter_to_rows(
        &self,
        batch: &RecordBatch,
        row_indices: &[usize],
    ) -> Result<RecordBatch, FillError> {
        let indices = UInt64Array::from(row_indices.iter().map(|&i| i as u64).collect::<Vec<_>>());

        let columns: Result<Vec<ArrayRef>, _> = batch
            .columns()
            .iter()
            .map(|col| datafusion::arrow::compute::take(col.as_ref(), &indices, None))
            .collect();

        let columns = columns.map_err(|e| FillError::ArrowError(e.to_string()))?;

        RecordBatch::try_new(batch.schema(), columns)
            .map_err(|e| FillError::ArrowError(e.to_string()))
    }

    /// Fill a single column according to the fill option
    fn fill_column(
        &self,
        col: &ArrayRef,
        expected_times: &[i64],
        existing_data: &HashMap<i64, usize>,
        is_time_col: bool,
    ) -> Result<ArrayRef, FillError> {
        match col.data_type() {
            DataType::Int64 => self.fill_int64_column(
                col.as_any().downcast_ref::<Int64Array>().unwrap(),
                expected_times,
                existing_data,
                is_time_col,
            ),
            DataType::Float64 => self.fill_float64_column(
                col.as_any().downcast_ref::<Float64Array>().unwrap(),
                expected_times,
                existing_data,
            ),
            DataType::Utf8 => self.fill_string_column(
                col.as_any().downcast_ref::<StringArray>().unwrap(),
                expected_times,
                existing_data,
            ),
            DataType::UInt64 => self.fill_uint64_column(
                col.as_any().downcast_ref::<UInt64Array>().unwrap(),
                expected_times,
                existing_data,
            ),
            _ => {
                // For unsupported types, just fill with nulls
                self.fill_with_nulls(col.data_type(), expected_times.len())
            }
        }
    }

    /// Fill Int64 column
    fn fill_int64_column(
        &self,
        col: &Int64Array,
        expected_times: &[i64],
        existing_data: &HashMap<i64, usize>,
        is_time_col: bool,
    ) -> Result<ArrayRef, FillError> {
        let mut builder = Int64Array::builder(expected_times.len());
        let mut last_value: Option<i64> = None;

        for (idx, &time) in expected_times.iter().enumerate() {
            if let Some(&row_idx) = existing_data.get(&time) {
                let val = if col.is_null(row_idx) {
                    None
                } else {
                    Some(col.value(row_idx))
                };
                last_value = val;
                if let Some(v) = val {
                    builder.append_value(v);
                } else {
                    builder.append_null();
                }
            } else if is_time_col {
                // Time column always gets the expected time value
                builder.append_value(time);
            } else {
                // Apply fill strategy
                match &self.fill_option {
                    FillOption::Null => builder.append_null(),
                    FillOption::None => builder.append_null(),
                    FillOption::Previous => {
                        if let Some(v) = last_value {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    }
                    FillOption::Linear => {
                        // Find next value for interpolation
                        let next_val =
                            self.find_next_value_int64(col, expected_times, existing_data, idx);
                        if let (Some(prev), Some(next)) = (last_value, next_val) {
                            let interpolated = (prev + next) / 2; // Simple linear
                            builder.append_value(interpolated);
                        } else {
                            builder.append_null();
                        }
                    }
                    FillOption::Value(v) => {
                        builder.append_value(*v as i64);
                    }
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    /// Find next non-null Int64 value for linear interpolation
    fn find_next_value_int64(
        &self,
        col: &Int64Array,
        expected_times: &[i64],
        existing_data: &HashMap<i64, usize>,
        current_idx: usize,
    ) -> Option<i64> {
        for &time in expected_times.iter().skip(current_idx + 1) {
            if let Some(&row_idx) = existing_data.get(&time) {
                if !col.is_null(row_idx) {
                    return Some(col.value(row_idx));
                }
            }
        }
        None
    }

    /// Fill Float64 column
    fn fill_float64_column(
        &self,
        col: &Float64Array,
        expected_times: &[i64],
        existing_data: &HashMap<i64, usize>,
    ) -> Result<ArrayRef, FillError> {
        let mut builder = Float64Array::builder(expected_times.len());
        let mut last_value: Option<f64> = None;
        let mut last_time: Option<i64> = None;

        for (idx, &time) in expected_times.iter().enumerate() {
            if let Some(&row_idx) = existing_data.get(&time) {
                let val = if col.is_null(row_idx) {
                    None
                } else {
                    Some(col.value(row_idx))
                };
                if val.is_some() {
                    last_value = val;
                    last_time = Some(time);
                }
                if let Some(v) = val {
                    builder.append_value(v);
                } else {
                    builder.append_null();
                }
            } else {
                match &self.fill_option {
                    FillOption::Null => builder.append_null(),
                    FillOption::None => builder.append_null(),
                    FillOption::Previous => {
                        if let Some(v) = last_value {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    }
                    FillOption::Linear => {
                        // True linear interpolation
                        if let (Some(prev_val), Some(prev_time)) = (last_value, last_time) {
                            if let Some((next_val, next_time)) = self.find_next_value_float64(
                                col,
                                expected_times,
                                existing_data,
                                idx,
                            ) {
                                let fraction =
                                    (time - prev_time) as f64 / (next_time - prev_time) as f64;
                                let interpolated = prev_val + (next_val - prev_val) * fraction;
                                builder.append_value(interpolated);
                            } else {
                                builder.append_null();
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    FillOption::Value(v) => {
                        builder.append_value(*v);
                    }
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    /// Find next non-null Float64 value for linear interpolation
    fn find_next_value_float64(
        &self,
        col: &Float64Array,
        expected_times: &[i64],
        existing_data: &HashMap<i64, usize>,
        current_idx: usize,
    ) -> Option<(f64, i64)> {
        for &time in expected_times.iter().skip(current_idx + 1) {
            if let Some(&row_idx) = existing_data.get(&time) {
                if !col.is_null(row_idx) {
                    return Some((col.value(row_idx), time));
                }
            }
        }
        None
    }

    /// Fill UInt64 column
    fn fill_uint64_column(
        &self,
        col: &UInt64Array,
        expected_times: &[i64],
        existing_data: &HashMap<i64, usize>,
    ) -> Result<ArrayRef, FillError> {
        let mut builder = UInt64Array::builder(expected_times.len());
        let mut last_value: Option<u64> = None;

        for &time in expected_times {
            if let Some(&row_idx) = existing_data.get(&time) {
                let val = if col.is_null(row_idx) {
                    None
                } else {
                    Some(col.value(row_idx))
                };
                last_value = val;
                if let Some(v) = val {
                    builder.append_value(v);
                } else {
                    builder.append_null();
                }
            } else {
                match &self.fill_option {
                    FillOption::Null => builder.append_null(),
                    FillOption::None => builder.append_null(),
                    FillOption::Previous => {
                        if let Some(v) = last_value {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    }
                    FillOption::Linear | FillOption::Value(_) => {
                        // Linear interpolation not meaningful for uint, use previous
                        if let Some(v) = last_value {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    /// Fill String column
    fn fill_string_column(
        &self,
        col: &StringArray,
        expected_times: &[i64],
        existing_data: &HashMap<i64, usize>,
    ) -> Result<ArrayRef, FillError> {
        let mut builder = StringBuilder::new();
        let mut last_value: Option<String> = None;

        for &time in expected_times {
            if let Some(&row_idx) = existing_data.get(&time) {
                let val = if col.is_null(row_idx) {
                    None
                } else {
                    Some(col.value(row_idx).to_string())
                };
                last_value = val.clone();
                if let Some(v) = val {
                    builder.append_value(&v);
                } else {
                    builder.append_null();
                }
            } else {
                match &self.fill_option {
                    FillOption::Null | FillOption::None | FillOption::Linear => {
                        builder.append_null()
                    }
                    FillOption::Previous => {
                        if let Some(ref v) = last_value {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    }
                    FillOption::Value(_) => {
                        // Can't fill string with number
                        builder.append_null();
                    }
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    /// Fill a column with a constant value (for group columns)
    fn fill_constant_column(
        &self,
        col: &ArrayRef,
        len: usize,
        group_key: &[String],
        col_idx: usize,
    ) -> Result<ArrayRef, FillError> {
        let key_idx = self
            .group_column_indices
            .iter()
            .position(|&idx| idx == col_idx)
            .unwrap_or(0);
        let val = &group_key[key_idx];

        match col.data_type() {
            DataType::Utf8 => {
                let arr: StringArray = (0..len).map(|_| Some(val.as_str())).collect();
                Ok(Arc::new(arr))
            }
            DataType::Int64 => {
                let v: i64 = val.parse().unwrap_or(0);
                let arr: Int64Array = (0..len).map(|_| Some(v)).collect();
                Ok(Arc::new(arr))
            }
            DataType::UInt64 => {
                let v: u64 = val.parse().unwrap_or(0);
                let arr: UInt64Array = (0..len).map(|_| Some(v)).collect();
                Ok(Arc::new(arr))
            }
            DataType::Float64 => {
                let v: f64 = val.parse().unwrap_or(0.0);
                let arr: Float64Array = (0..len).map(|_| Some(v)).collect();
                Ok(Arc::new(arr))
            }
            _ => self.fill_with_nulls(col.data_type(), len),
        }
    }

    /// Fill with nulls for unsupported types
    fn fill_with_nulls(&self, data_type: &DataType, len: usize) -> Result<ArrayRef, FillError> {
        match data_type {
            DataType::Int64 => {
                let arr: Int64Array = (0..len).map(|_| None::<i64>).collect();
                Ok(Arc::new(arr))
            }
            DataType::Float64 => {
                let arr: Float64Array = (0..len).map(|_| None::<f64>).collect();
                Ok(Arc::new(arr))
            }
            DataType::Utf8 => {
                let arr: StringArray = (0..len).map(|_| None::<&str>).collect();
                Ok(Arc::new(arr))
            }
            DataType::UInt64 => {
                let arr: UInt64Array = (0..len).map(|_| None::<u64>).collect();
                Ok(Arc::new(arr))
            }
            DataType::Boolean => {
                let arr: BooleanArray = (0..len).map(|_| None::<bool>).collect();
                Ok(Arc::new(arr))
            }
            _ => Err(FillError::UnsupportedDataType(format!("{:?}", data_type))),
        }
    }
}

use datafusion::arrow::array::StringBuilder;

/// Errors that can occur during fill processing
#[derive(Debug, Clone)]
pub enum FillError {
    InvalidTimeColumn,
    NoValidTimeValues,
    ArrowError(String),
    UnsupportedDataType(String),
}

impl std::fmt::Display for FillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTimeColumn => write!(f, "Invalid time column type (expected Int64)"),
            Self::NoValidTimeValues => write!(f, "No valid time values found in result"),
            Self::ArrowError(e) => write!(f, "Arrow error: {}", e),
            Self::UnsupportedDataType(t) => write!(f, "Unsupported data type for fill: {}", t),
        }
    }
}

impl std::error::Error for FillError {}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;

    const NANOS_PER_MINUTE: i64 = 60_000_000_000;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn create_test_batch(times: &[i64], values: &[Option<f64>]) -> RecordBatch {
        let schema = create_test_schema();
        let time_arr = Int64Array::from(times.to_vec());
        let value_arr = Float64Array::from(values.to_vec());

        RecordBatch::try_new(schema, vec![Arc::new(time_arr), Arc::new(value_arr)]).unwrap()
    }

    #[test]
    fn test_fill_none_returns_unchanged() {
        let batch = create_test_batch(&[0, 5 * NANOS_PER_MINUTE], &[Some(1.0), Some(2.0)]);

        let processor = FillProcessor::new(5 * NANOS_PER_MINUTE, FillOption::None, 0, vec![]);

        let result = processor.apply(vec![batch.clone()]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[test]
    fn test_fill_null_inserts_nulls() {
        // Data has a gap at 5m
        let batch = create_test_batch(&[0, 10 * NANOS_PER_MINUTE], &[Some(1.0), Some(3.0)]);

        let processor = FillProcessor::new(5 * NANOS_PER_MINUTE, FillOption::Null, 0, vec![]);

        let result = processor.apply(vec![batch]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3); // 0, 5m, 10m

        let values = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1.0);
        assert!(values.is_null(1)); // Gap filled with null
        assert_eq!(values.value(2), 3.0);
    }

    #[test]
    fn test_fill_previous() {
        let batch = create_test_batch(&[0, 10 * NANOS_PER_MINUTE], &[Some(1.0), Some(3.0)]);

        let processor = FillProcessor::new(5 * NANOS_PER_MINUTE, FillOption::Previous, 0, vec![]);

        let result = processor.apply(vec![batch]).unwrap();
        let values = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(values.value(0), 1.0);
        assert_eq!(values.value(1), 1.0); // Forward filled
        assert_eq!(values.value(2), 3.0);
    }

    #[test]
    fn test_fill_linear() {
        let batch = create_test_batch(&[0, 10 * NANOS_PER_MINUTE], &[Some(0.0), Some(10.0)]);

        let processor = FillProcessor::new(5 * NANOS_PER_MINUTE, FillOption::Linear, 0, vec![]);

        let result = processor.apply(vec![batch]).unwrap();
        let values = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(values.value(0), 0.0);
        assert_eq!(values.value(1), 5.0); // Linearly interpolated
        assert_eq!(values.value(2), 10.0);
    }

    #[test]
    fn test_fill_value() {
        let batch = create_test_batch(&[0, 10 * NANOS_PER_MINUTE], &[Some(1.0), Some(3.0)]);

        let processor =
            FillProcessor::new(5 * NANOS_PER_MINUTE, FillOption::Value(99.0), 0, vec![]);

        let result = processor.apply(vec![batch]).unwrap();
        let values = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(values.value(0), 1.0);
        assert_eq!(values.value(1), 99.0); // Filled with constant
        assert_eq!(values.value(2), 3.0);
    }

    #[test]
    fn test_time_column_always_filled() {
        let batch = create_test_batch(&[0, 10 * NANOS_PER_MINUTE], &[Some(1.0), Some(3.0)]);

        let processor = FillProcessor::new(5 * NANOS_PER_MINUTE, FillOption::Null, 0, vec![]);

        let result = processor.apply(vec![batch]).unwrap();
        let times = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(times.value(0), 0);
        assert_eq!(times.value(1), 5 * NANOS_PER_MINUTE); // Gap time filled
        assert_eq!(times.value(2), 10 * NANOS_PER_MINUTE);
    }
}
