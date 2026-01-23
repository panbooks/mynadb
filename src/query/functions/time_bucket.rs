//! Time Bucket UDF
//!
//! Provides a vectorized `time_bucket` function for time-series bucketing.
//! This aligns timestamps to preset round-number boundaries.
//!
//! Formula: `floor((timestamp - offset) / interval) * interval + offset`
//!
//! Example:
//! - With interval=5m and offset=0:
//!   - 10:03:00 → 10:00:00
//!   - 10:07:30 → 10:05:00
//!   - 10:10:00 → 10:10:00

use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// Create the time_bucket UDF
///
/// Usage: `time_bucket(timestamp_column, interval_nanos, offset_nanos)`
///
/// Arguments:
/// - `timestamp_column`: Int64 timestamp in nanoseconds
/// - `interval_nanos`: Int64 bucket interval in nanoseconds
/// - `offset_nanos`: Int64 offset for bucket alignment in nanoseconds
///
/// Returns: Int64 bucketed timestamp in nanoseconds
pub fn create_time_bucket_udf() -> ScalarUDF {
    ScalarUDF::from(TimeBucketUDF::new())
}

/// TimeBucket UDF implementation
#[derive(Debug)]
struct TimeBucketUDF {
    signature: Signature,
}

impl TimeBucketUDF {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TimeBucketUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "time_bucket"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 3 {
            return Err(datafusion::common::DataFusionError::Execution(
                "time_bucket requires 3 arguments: timestamp, interval_nanos, offset_nanos"
                    .to_string(),
            ));
        }

        // Extract arguments
        let timestamps = &args[0];
        let interval_nanos = &args[1];
        let offset_nanos = &args[2];

        // Handle different combinations of scalar/array arguments
        match (timestamps, interval_nanos, offset_nanos) {
            // All scalars
            (
                ColumnarValue::Scalar(ts),
                ColumnarValue::Scalar(interval),
                ColumnarValue::Scalar(offset),
            ) => {
                let ts_val = scalar_to_i64(ts)?;
                let interval_val = scalar_to_i64(interval)?;
                let offset_val = scalar_to_i64(offset)?;

                let bucketed = compute_bucket(ts_val, interval_val, offset_val);
                Ok(ColumnarValue::Scalar(
                    datafusion::common::ScalarValue::Int64(Some(bucketed)),
                ))
            }

            // Timestamp is array, interval and offset are scalars (most common case)
            (
                ColumnarValue::Array(ts_arr),
                ColumnarValue::Scalar(interval),
                ColumnarValue::Scalar(offset),
            ) => {
                let interval_val = scalar_to_i64(interval)?;
                let offset_val = scalar_to_i64(offset)?;

                let ts_arr = ts_arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution(
                            "time_bucket: timestamp must be Int64".to_string(),
                        )
                    })?;

                // Vectorized computation
                let result: Int64Array = ts_arr
                    .iter()
                    .map(|opt_ts| opt_ts.map(|ts| compute_bucket(ts, interval_val, offset_val)))
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // All arrays
            (
                ColumnarValue::Array(ts_arr),
                ColumnarValue::Array(interval_arr),
                ColumnarValue::Array(offset_arr),
            ) => {
                let ts_arr = ts_arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution(
                            "time_bucket: timestamp must be Int64".to_string(),
                        )
                    })?;
                let interval_arr = interval_arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution(
                            "time_bucket: interval must be Int64".to_string(),
                        )
                    })?;
                let offset_arr = offset_arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution(
                            "time_bucket: offset must be Int64".to_string(),
                        )
                    })?;

                let result: Int64Array = (0..ts_arr.len())
                    .map(|i| {
                        if ts_arr.is_null(i) || interval_arr.is_null(i) || offset_arr.is_null(i) {
                            None
                        } else {
                            Some(compute_bucket(
                                ts_arr.value(i),
                                interval_arr.value(i),
                                offset_arr.value(i),
                            ))
                        }
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // Mixed cases - convert scalars to arrays
            _ => {
                // Get the array length from any array argument
                let len = [timestamps, interval_nanos, offset_nanos]
                    .iter()
                    .find_map(|cv| match cv {
                        ColumnarValue::Array(arr) => Some(arr.len()),
                        _ => None,
                    })
                    .unwrap_or(1);

                let ts_arr = columnar_to_array(timestamps, len)?;
                let interval_arr = columnar_to_array(interval_nanos, len)?;
                let offset_arr = columnar_to_array(offset_nanos, len)?;

                let ts_arr = ts_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "time_bucket: timestamp must be Int64".to_string(),
                    )
                })?;
                let interval_arr =
                    interval_arr
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            datafusion::common::DataFusionError::Execution(
                                "time_bucket: interval must be Int64".to_string(),
                            )
                        })?;
                let offset_arr =
                    offset_arr
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            datafusion::common::DataFusionError::Execution(
                                "time_bucket: offset must be Int64".to_string(),
                            )
                        })?;

                let result: Int64Array = (0..len)
                    .map(|i| {
                        if ts_arr.is_null(i) || interval_arr.is_null(i) || offset_arr.is_null(i) {
                            None
                        } else {
                            Some(compute_bucket(
                                ts_arr.value(i),
                                interval_arr.value(i),
                                offset_arr.value(i),
                            ))
                        }
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}

/// Compute the bucket for a single timestamp
///
/// Formula: `floor((timestamp - offset) / interval) * interval + offset`
#[inline]
fn compute_bucket(timestamp: i64, interval: i64, offset: i64) -> i64 {
    if interval <= 0 {
        return timestamp; // No bucketing for invalid interval
    }

    // Adjust for offset
    let adjusted = timestamp - offset;

    // Floor division for negative values
    let bucket_num = if adjusted >= 0 {
        adjusted / interval
    } else {
        (adjusted - interval + 1) / interval
    };

    bucket_num * interval + offset
}

/// Extract i64 value from a ScalarValue
fn scalar_to_i64(scalar: &datafusion::common::ScalarValue) -> Result<i64> {
    match scalar {
        datafusion::common::ScalarValue::Int64(Some(v)) => Ok(*v),
        datafusion::common::ScalarValue::Int32(Some(v)) => Ok(*v as i64),
        datafusion::common::ScalarValue::UInt64(Some(v)) => Ok(*v as i64),
        datafusion::common::ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
        _ => Err(datafusion::common::DataFusionError::Execution(format!(
            "time_bucket: expected integer, got {:?}",
            scalar
        ))),
    }
}

/// Convert ColumnarValue to ArrayRef, expanding scalars if needed
fn columnar_to_array(cv: &ColumnarValue, len: usize) -> Result<ArrayRef> {
    match cv {
        ColumnarValue::Array(arr) => Ok(arr.clone()),
        ColumnarValue::Scalar(scalar) => {
            let arr = scalar.to_array_of_size(len)?;
            Ok(arr)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;

    const NANOS_PER_MINUTE: i64 = 60_000_000_000;
    const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MINUTE;

    #[test]
    fn test_compute_bucket_5m_aligned() {
        let interval = 5 * NANOS_PER_MINUTE;
        let offset = 0;

        // 10:00:00 should stay at 10:00:00
        let ts_10_00 = 10 * NANOS_PER_HOUR;
        assert_eq!(compute_bucket(ts_10_00, interval, offset), ts_10_00);

        // 10:03:00 should bucket to 10:00:00
        let ts_10_03 = 10 * NANOS_PER_HOUR + 3 * NANOS_PER_MINUTE;
        assert_eq!(compute_bucket(ts_10_03, interval, offset), ts_10_00);

        // 10:05:00 should be at 10:05:00
        let ts_10_05 = 10 * NANOS_PER_HOUR + 5 * NANOS_PER_MINUTE;
        assert_eq!(compute_bucket(ts_10_05, interval, offset), ts_10_05);

        // 10:07:30 should bucket to 10:05:00
        let ts_10_07_30 = 10 * NANOS_PER_HOUR + 7 * NANOS_PER_MINUTE + 30_000_000_000;
        assert_eq!(compute_bucket(ts_10_07_30, interval, offset), ts_10_05);
    }

    #[test]
    fn test_compute_bucket_with_offset() {
        let interval = 5 * NANOS_PER_MINUTE;
        let offset = 2 * NANOS_PER_MINUTE; // 2 minute offset

        // With offset=2m, buckets are at :02, :07, :12, etc.
        // 10:03:00 should bucket to 10:02:00
        let ts_10_03 = 10 * NANOS_PER_HOUR + 3 * NANOS_PER_MINUTE;
        let expected = 10 * NANOS_PER_HOUR + 2 * NANOS_PER_MINUTE;
        assert_eq!(compute_bucket(ts_10_03, interval, offset), expected);

        // 10:08:00 should bucket to 10:07:00
        let ts_10_08 = 10 * NANOS_PER_HOUR + 8 * NANOS_PER_MINUTE;
        let expected = 10 * NANOS_PER_HOUR + 7 * NANOS_PER_MINUTE;
        assert_eq!(compute_bucket(ts_10_08, interval, offset), expected);
    }

    #[test]
    fn test_compute_bucket_1h() {
        let interval = NANOS_PER_HOUR;
        let offset = 0;

        // 10:30:00 should bucket to 10:00:00
        let ts_10_30 = 10 * NANOS_PER_HOUR + 30 * NANOS_PER_MINUTE;
        let expected = 10 * NANOS_PER_HOUR;
        assert_eq!(compute_bucket(ts_10_30, interval, offset), expected);

        // 11:00:00 should be at 11:00:00
        let ts_11_00 = 11 * NANOS_PER_HOUR;
        assert_eq!(compute_bucket(ts_11_00, interval, offset), ts_11_00);
    }

    #[test]
    fn test_time_bucket_udf_scalar() {
        let udf = create_time_bucket_udf();

        let ts = ColumnarValue::Scalar(ScalarValue::Int64(Some(
            10 * NANOS_PER_HOUR + 3 * NANOS_PER_MINUTE,
        )));
        let interval = ColumnarValue::Scalar(ScalarValue::Int64(Some(5 * NANOS_PER_MINUTE)));
        let offset = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));

        let result = udf.invoke(&[ts, interval, offset]).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
                assert_eq!(v, 10 * NANOS_PER_HOUR);
            }
            _ => panic!("Expected scalar Int64 result"),
        }
    }

    #[test]
    fn test_time_bucket_udf_array() {
        let udf = create_time_bucket_udf();

        let timestamps = Int64Array::from(vec![
            10 * NANOS_PER_HOUR,
            10 * NANOS_PER_HOUR + 3 * NANOS_PER_MINUTE,
            10 * NANOS_PER_HOUR + 5 * NANOS_PER_MINUTE,
            10 * NANOS_PER_HOUR + 7 * NANOS_PER_MINUTE,
        ]);

        let ts = ColumnarValue::Array(Arc::new(timestamps));
        let interval = ColumnarValue::Scalar(ScalarValue::Int64(Some(5 * NANOS_PER_MINUTE)));
        let offset = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));

        let result = udf.invoke(&[ts, interval, offset]).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(arr.len(), 4);
                assert_eq!(arr.value(0), 10 * NANOS_PER_HOUR);
                assert_eq!(arr.value(1), 10 * NANOS_PER_HOUR);
                assert_eq!(arr.value(2), 10 * NANOS_PER_HOUR + 5 * NANOS_PER_MINUTE);
                assert_eq!(arr.value(3), 10 * NANOS_PER_HOUR + 5 * NANOS_PER_MINUTE);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_time_bucket_udf_with_nulls() {
        let udf = create_time_bucket_udf();

        let timestamps =
            Int64Array::from(vec![Some(10 * NANOS_PER_HOUR + 3 * NANOS_PER_MINUTE), None]);

        let ts = ColumnarValue::Array(Arc::new(timestamps));
        let interval = ColumnarValue::Scalar(ScalarValue::Int64(Some(5 * NANOS_PER_MINUTE)));
        let offset = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));

        let result = udf.invoke(&[ts, interval, offset]).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(arr.len(), 2);
                assert_eq!(arr.value(0), 10 * NANOS_PER_HOUR);
                assert!(arr.is_null(1));
            }
            _ => panic!("Expected array result"),
        }
    }
}
