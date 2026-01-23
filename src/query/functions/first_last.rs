//! First/Last Value UDAFs
//!
//! Provides `first_value` and `last_value` aggregate functions compatible with InfluxDB 1.x.
//!
//! - `first_value(value, timestamp)` - Returns the value with the minimum timestamp in each group
//! - `last_value(value, timestamp)` - Returns the value with the maximum timestamp in each group
//!
//! Example:
//! ```sql
//! SELECT first_value(usage, timestamp) as first_usage,
//!        last_value(usage, timestamp) as last_usage
//! FROM cpu
//! GROUP BY time_bucket(timestamp, 300000000000, 0)
//! ```

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    function::AccumulatorArgs, Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use std::any::Any;

// ============================================================================
// First Value UDAF
// ============================================================================

/// Create the first_value UDAF
///
/// Usage: `first_value(value_column, timestamp_column)`
///
/// Returns the value from the row with the minimum timestamp in each group.
pub fn create_first_value_udaf() -> AggregateUDF {
    AggregateUDF::from(FirstValueUDAF::new())
}

/// FirstValue UDAF implementation
#[derive(Debug, Clone)]
struct FirstValueUDAF {
    signature: Signature,
}

impl FirstValueUDAF {
    fn new() -> Self {
        // Accept any combination of (value_type, timestamp_type)
        // The value can be Float64, Int64, or Utf8
        // The timestamp should be Int64 (nanoseconds)
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for FirstValueUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "first_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Return type matches the first argument (value column)
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(
                "first_value requires at least one argument".to_string(),
            ));
        }
        Ok(arg_types[0].clone())
    }

    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> Result<Vec<datafusion::arrow::datatypes::Field>> {
        use datafusion::arrow::datatypes::Field;
        let value_type = args.input_types[0].clone();
        Ok(vec![
            Field::new("value", value_type, true),
            Field::new("timestamp", DataType::Int64, true),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let value_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        Ok(Box::new(FirstValueAccumulator::new(value_type)))
    }
}

/// Accumulator for first_value
///
/// Tracks the value with the minimum timestamp seen so far.
#[derive(Debug)]
struct FirstValueAccumulator {
    /// The value type for this accumulator
    value_type: DataType,
    /// Current state: (value, min_timestamp)
    state: Option<(ScalarValue, i64)>,
}

impl FirstValueAccumulator {
    fn new(value_type: DataType) -> Self {
        Self {
            value_type,
            state: None,
        }
    }
}

impl Accumulator for FirstValueAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 2 {
            return Err(DataFusionError::Execution(
                "first_value requires exactly 2 arguments: value, timestamp".to_string(),
            ));
        }

        let value_array = &values[0];
        let timestamp_array = &values[1];

        // Get timestamps as i64
        let timestamps = extract_timestamps(timestamp_array)?;

        for i in 0..value_array.len() {
            if value_array.is_null(i) || timestamps.is_null(i) {
                continue;
            }

            let ts = timestamps.value(i);
            let should_update = match &self.state {
                None => true,
                Some((_, min_ts)) => ts < *min_ts,
            };

            if should_update {
                let value = extract_scalar_value(value_array, i, &self.value_type)?;
                self.state = Some((value, ts));
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // States are: [values, timestamps]
        if states.len() != 2 {
            return Ok(());
        }

        let value_array = &states[0];
        let timestamp_array = &states[1];
        let timestamps = extract_timestamps(timestamp_array)?;

        for i in 0..value_array.len() {
            if value_array.is_null(i) || timestamps.is_null(i) {
                continue;
            }

            let ts = timestamps.value(i);
            let should_update = match &self.state {
                None => true,
                Some((_, min_ts)) => ts < *min_ts,
            };

            if should_update {
                let value = extract_scalar_value(value_array, i, &self.value_type)?;
                self.state = Some((value, ts));
            }
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Return [value, timestamp] as state for merging
        match &self.state {
            Some((value, ts)) => Ok(vec![value.clone(), ScalarValue::Int64(Some(*ts))]),
            None => Ok(vec![
                ScalarValue::try_from(&self.value_type)?,
                ScalarValue::Int64(None),
            ]),
        }
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match &self.state {
            Some((value, _)) => Ok(value.clone()),
            None => ScalarValue::try_from(&self.value_type),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

// ============================================================================
// Last Value UDAF
// ============================================================================

/// Create the last_value UDAF
///
/// Usage: `last_value(value_column, timestamp_column)`
///
/// Returns the value from the row with the maximum timestamp in each group.
pub fn create_last_value_udaf() -> AggregateUDF {
    AggregateUDF::from(LastValueUDAF::new())
}

/// LastValue UDAF implementation
#[derive(Debug, Clone)]
struct LastValueUDAF {
    signature: Signature,
}

impl LastValueUDAF {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for LastValueUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "last_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(
                "last_value requires at least one argument".to_string(),
            ));
        }
        Ok(arg_types[0].clone())
    }

    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> Result<Vec<datafusion::arrow::datatypes::Field>> {
        use datafusion::arrow::datatypes::Field;
        let value_type = args.input_types[0].clone();
        Ok(vec![
            Field::new("value", value_type, true),
            Field::new("timestamp", DataType::Int64, true),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let value_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        Ok(Box::new(LastValueAccumulator::new(value_type)))
    }
}

/// Accumulator for last_value
///
/// Tracks the value with the maximum timestamp seen so far.
#[derive(Debug)]
struct LastValueAccumulator {
    /// The value type for this accumulator
    value_type: DataType,
    /// Current state: (value, max_timestamp)
    state: Option<(ScalarValue, i64)>,
}

impl LastValueAccumulator {
    fn new(value_type: DataType) -> Self {
        Self {
            value_type,
            state: None,
        }
    }
}

impl Accumulator for LastValueAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 2 {
            return Err(DataFusionError::Execution(
                "last_value requires exactly 2 arguments: value, timestamp".to_string(),
            ));
        }

        let value_array = &values[0];
        let timestamp_array = &values[1];

        // Get timestamps as i64
        let timestamps = extract_timestamps(timestamp_array)?;

        for i in 0..value_array.len() {
            if value_array.is_null(i) || timestamps.is_null(i) {
                continue;
            }

            let ts = timestamps.value(i);
            let should_update = match &self.state {
                None => true,
                Some((_, max_ts)) => ts > *max_ts,
            };

            if should_update {
                let value = extract_scalar_value(value_array, i, &self.value_type)?;
                self.state = Some((value, ts));
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            return Ok(());
        }

        let value_array = &states[0];
        let timestamp_array = &states[1];
        let timestamps = extract_timestamps(timestamp_array)?;

        for i in 0..value_array.len() {
            if value_array.is_null(i) || timestamps.is_null(i) {
                continue;
            }

            let ts = timestamps.value(i);
            let should_update = match &self.state {
                None => true,
                Some((_, max_ts)) => ts > *max_ts,
            };

            if should_update {
                let value = extract_scalar_value(value_array, i, &self.value_type)?;
                self.state = Some((value, ts));
            }
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        match &self.state {
            Some((value, ts)) => Ok(vec![value.clone(), ScalarValue::Int64(Some(*ts))]),
            None => Ok(vec![
                ScalarValue::try_from(&self.value_type)?,
                ScalarValue::Int64(None),
            ]),
        }
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match &self.state {
            Some((value, _)) => Ok(value.clone()),
            None => ScalarValue::try_from(&self.value_type),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Extract timestamps from an array as Int64Array
fn extract_timestamps(array: &ArrayRef) -> Result<Int64Array> {
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("Failed to downcast to Int64Array".to_string())
            })?;
            Ok(arr.clone())
        }
        DataType::Timestamp(unit, _) => {
            use datafusion::arrow::datatypes::TimeUnit;
            match unit {
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "Failed to downcast to TimestampNanosecondArray".to_string(),
                            )
                        })?;
                    Ok(Int64Array::from_iter(arr.iter()))
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "Failed to downcast to TimestampMicrosecondArray".to_string(),
                            )
                        })?;
                    Ok(Int64Array::from_iter(
                        arr.iter().map(|v| v.map(|x| x * 1000)),
                    ))
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "Failed to downcast to TimestampMillisecondArray".to_string(),
                            )
                        })?;
                    Ok(Int64Array::from_iter(
                        arr.iter().map(|v| v.map(|x| x * 1_000_000)),
                    ))
                }
                TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::TimestampSecondArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "Failed to downcast to TimestampSecondArray".to_string(),
                            )
                        })?;
                    Ok(Int64Array::from_iter(
                        arr.iter().map(|v| v.map(|x| x * 1_000_000_000)),
                    ))
                }
            }
        }
        other => Err(DataFusionError::Execution(format!(
            "first_value/last_value: unsupported timestamp type: {:?}",
            other
        ))),
    }
}

/// Extract a scalar value from an array at a specific index
fn extract_scalar_value(
    array: &ArrayRef,
    index: usize,
    expected_type: &DataType,
) -> Result<ScalarValue> {
    match expected_type {
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Failed to downcast to Float64Array".to_string())
                })?;
            Ok(ScalarValue::Float64(Some(arr.value(index))))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("Failed to downcast to Int64Array".to_string())
            })?;
            Ok(ScalarValue::Int64(Some(arr.value(index))))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Failed to downcast to StringArray".to_string())
                })?;
            Ok(ScalarValue::Utf8(Some(arr.value(index).to_string())))
        }
        other => Err(DataFusionError::Execution(format!(
            "first_value/last_value: unsupported value type: {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::prelude::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_first_value_basic() {
        let ctx = SessionContext::new();

        // Register the UDAF
        ctx.register_udaf(create_first_value_udaf());

        // Create test data: values with timestamps
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Float64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0])),
                Arc::new(Int64Array::from(vec![300, 100, 400, 200])), // timestamps
            ],
        )
        .unwrap();

        ctx.register_batch("test", batch).unwrap();

        // Query: first_value should return 20.0 (value at timestamp 100)
        let df = ctx
            .sql("SELECT first_value(value, timestamp) as first_val FROM test")
            .await
            .unwrap();

        let results = df.collect().await.unwrap();
        assert_eq!(results.len(), 1);

        let arr = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 20.0);
    }

    #[tokio::test]
    async fn test_last_value_basic() {
        let ctx = SessionContext::new();

        // Register the UDAF
        ctx.register_udaf(create_last_value_udaf());

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Float64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0])),
                Arc::new(Int64Array::from(vec![300, 100, 400, 200])), // timestamps
            ],
        )
        .unwrap();

        ctx.register_batch("test", batch).unwrap();

        // Query: last_value should return 30.0 (value at timestamp 400)
        let df = ctx
            .sql("SELECT last_value(value, timestamp) as last_val FROM test")
            .await
            .unwrap();

        let results = df.collect().await.unwrap();
        assert_eq!(results.len(), 1);

        let arr = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 30.0);
    }

    #[tokio::test]
    async fn test_first_last_with_group_by() {
        let ctx = SessionContext::new();

        ctx.register_udaf(create_first_value_udaf());
        ctx.register_udaf(create_last_value_udaf());

        // Create test data with groups
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b", "b"])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0])),
                Arc::new(Int64Array::from(vec![100, 200, 150, 50])),
            ],
        )
        .unwrap();

        ctx.register_batch("test", batch).unwrap();

        let df = ctx
            .sql(
                "SELECT host, first_value(value, timestamp) as first_val, \
                 last_value(value, timestamp) as last_val \
                 FROM test GROUP BY host ORDER BY host",
            )
            .await
            .unwrap();

        let results = df.collect().await.unwrap();
        assert_eq!(results.len(), 1);

        // Host 'a': first=10.0 (ts=100), last=20.0 (ts=200)
        // Host 'b': first=40.0 (ts=50), last=30.0 (ts=150)
        let first_vals = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let last_vals = results[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(first_vals.value(0), 10.0); // host 'a' first
        assert_eq!(last_vals.value(0), 20.0); // host 'a' last
        assert_eq!(first_vals.value(1), 40.0); // host 'b' first
        assert_eq!(last_vals.value(1), 30.0); // host 'b' last
    }
}
