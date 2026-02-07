//! Time-Series Forecasting UDAFs
//!
//! Built-in forecasting models exposed as DataFusion aggregate functions.
//! Each UDAF collects `(time, value)` pairs during aggregation, fits a model
//! in `evaluate()`, and returns predicted value(s).
//!
//! Functions:
//! - `forecast_linear(value, time, steps)` → Float64 — OLS linear regression
//! - `forecast_ema(value, time, steps, alpha)` → Float64 — Exponential moving average
//! - `forecast_arima(value, time, steps, p, d, q)` → Float64 — ARIMA(p,d,q)
//! - `forecast_auto(value, time, steps)` → Float64 — Best-of linear/EMA by fit
//! - `forecast_series(value, time, steps, model)` → Utf8 — Multi-step JSON forecast
//!
//! Example:
//! ```sql
//! SELECT host,
//!        forecast_linear(cpu_usage, time, 3) as predicted_3_steps,
//!        forecast_arima(cpu_usage, time, 1, 1, 1, 0) as arima_next
//! FROM metrics
//! WHERE time > now() - interval '1 hour'
//! GROUP BY host;
//! ```

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    function::AccumulatorArgs, Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use std::any::Any;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Collected time-series data for one aggregation group.
#[derive(Debug, Clone)]
struct TimeSeriesData {
    times: Vec<i64>,
    values: Vec<f64>,
}

impl TimeSeriesData {
    fn new() -> Self {
        Self {
            times: Vec::new(),
            values: Vec::new(),
        }
    }

    /// Ingest a batch of (value, time) arrays, skipping nulls.
    fn ingest(&mut self, value_arr: &ArrayRef, time_arr: &ArrayRef) -> Result<()> {
        let values = value_arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("forecast: value column must be Float64".into())
            })?;
        let times = extract_i64(time_arr)?;

        for i in 0..values.len() {
            if values.is_null(i) || times.is_null(i) {
                continue;
            }
            self.times.push(times.value(i));
            self.values.push(values.value(i));
        }
        Ok(())
    }

    /// Sort in-place by time.
    fn sorted(&mut self) {
        let mut indices: Vec<usize> = (0..self.times.len()).collect();
        indices.sort_by_key(|&i| self.times[i]);
        let sorted_times: Vec<i64> = indices.iter().map(|&i| self.times[i]).collect();
        let sorted_values: Vec<f64> = indices.iter().map(|&i| self.values[i]).collect();
        self.times = sorted_times;
        self.values = sorted_values;
    }

    fn len(&self) -> usize {
        self.times.len()
    }

    fn is_empty(&self) -> bool {
        self.times.is_empty()
    }

    /// Serialize to JSON for state transfer.
    fn to_json(&self) -> String {
        // Simple JSON: {"t":[...],"v":[...]}
        let times_str: Vec<String> = self.times.iter().map(|t| t.to_string()).collect();
        let values_str: Vec<String> = self.values.iter().map(|v| format!("{v}")).collect();
        format!(
            "{{\"t\":[{}],\"v\":[{}]}}",
            times_str.join(","),
            values_str.join(",")
        )
    }

    /// Deserialize from JSON.
    fn from_json(s: &str) -> Result<Self> {
        // Parse {"t":[...],"v":[...]}
        let parsed: serde_json::Value = serde_json::from_str(s).map_err(|e| {
            DataFusionError::Execution(format!("forecast: failed to parse state JSON: {e}"))
        })?;
        let times = parsed["t"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_i64())
            .collect();
        let values = parsed["v"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_f64())
            .collect();
        Ok(Self { times, values })
    }

    /// Merge another TimeSeriesData into this one.
    fn merge(&mut self, other: &TimeSeriesData) {
        self.times.extend_from_slice(&other.times);
        self.values.extend_from_slice(&other.values);
    }
}

/// Extract i64 values from an array (supports Int64 and Timestamp types).
fn extract_i64(array: &ArrayRef) -> Result<Int64Array> {
    match array.data_type() {
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Failed to downcast to Int64Array".into())
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
                            DataFusionError::Execution("Failed to downcast timestamp".into())
                        })?;
                    Ok(Int64Array::from_iter(arr.iter()))
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("Failed to downcast timestamp".into())
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
                            DataFusionError::Execution("Failed to downcast timestamp".into())
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
                            DataFusionError::Execution("Failed to downcast timestamp".into())
                        })?;
                    Ok(Int64Array::from_iter(
                        arr.iter().map(|v| v.map(|x| x * 1_000_000_000)),
                    ))
                }
            }
        }
        other => Err(DataFusionError::Execution(format!(
            "forecast: unsupported time type: {other:?}"
        ))),
    }
}

/// Extract the first non-null i64 from an ArrayRef. Returns None if empty/all-null.
fn first_i64_opt(arr: &ArrayRef) -> Option<i64> {
    let a = arr.as_any().downcast_ref::<Int64Array>()?;
    for i in 0..a.len() {
        if !a.is_null(i) {
            return Some(a.value(i));
        }
    }
    None
}

/// Extract the first non-null f64 from an ArrayRef. Returns None if empty/all-null.
fn first_f64_opt(arr: &ArrayRef) -> Option<f64> {
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        for i in 0..a.len() {
            if !a.is_null(i) {
                return Some(a.value(i));
            }
        }
    } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        for i in 0..a.len() {
            if !a.is_null(i) {
                return Some(a.value(i) as f64);
            }
        }
    }
    None
}

/// Extract the first non-null string from an ArrayRef. Returns None if empty/all-null.
fn first_string_opt(arr: &ArrayRef) -> Option<String> {
    let a = arr.as_any().downcast_ref::<StringArray>()?;
    for i in 0..a.len() {
        if !a.is_null(i) {
            return Some(a.value(i).to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Forecasting models (pure functions)
// ---------------------------------------------------------------------------

/// OLS linear regression on normalized time. Returns (slope, intercept, t0).
/// Model: y = slope * (t - t0) + intercept
fn fit_linear(times: &[i64], values: &[f64]) -> (f64, f64, f64) {
    let n = times.len() as f64;
    if n < 2.0 {
        return (0.0, values.first().copied().unwrap_or(0.0), times.first().copied().unwrap_or(0) as f64);
    }
    // Normalize times to avoid precision loss with large nanosecond timestamps
    let t0 = times[0] as f64;
    let sum_t: f64 = times.iter().map(|&t| t as f64 - t0).sum();
    let sum_v: f64 = values.iter().sum();
    let sum_tv: f64 = times
        .iter()
        .zip(values.iter())
        .map(|(&t, &v)| (t as f64 - t0) * v)
        .sum();
    let sum_t2: f64 = times.iter().map(|&t| (t as f64 - t0).powi(2)).sum();

    let denom = n * sum_t2 - sum_t * sum_t;
    if denom.abs() < 1e-15 {
        return (0.0, sum_v / n, t0);
    }
    let slope = (n * sum_tv - sum_t * sum_v) / denom;
    let intercept = (sum_v - slope * sum_t) / n;
    (slope, intercept, t0)
}

/// Predict using linear model at a future time.
fn predict_linear(slope: f64, intercept: f64, t0: f64, target_time: f64) -> f64 {
    slope * (target_time - t0) + intercept
}

/// Compute mean squared error for linear regression.
fn linear_mse(times: &[i64], values: &[f64]) -> f64 {
    if values.len() < 2 {
        return f64::MAX;
    }
    let (slope, intercept, t0) = fit_linear(times, values);
    let sse: f64 = times
        .iter()
        .zip(values.iter())
        .map(|(&t, &v)| {
            let pred = predict_linear(slope, intercept, t0, t as f64);
            (v - pred).powi(2)
        })
        .sum();
    sse / values.len() as f64
}

/// Forecast N steps ahead using linear regression.
fn forecast_linear_steps(times: &[i64], values: &[f64], steps: usize) -> Vec<f64> {
    if values.is_empty() {
        return vec![0.0; steps];
    }
    if values.len() == 1 {
        return vec![values[0]; steps];
    }

    let (slope, intercept, t0) = fit_linear(times, values);
    let n = times.len();

    // Estimate time step interval
    let avg_interval = (times[n - 1] - times[0]) as f64 / (n - 1) as f64;

    (1..=steps)
        .map(|step| {
            let future_t = times[n - 1] as f64 + avg_interval * step as f64;
            predict_linear(slope, intercept, t0, future_t)
        })
        .collect()
}

/// Forecast N steps ahead using exponential moving average.
fn forecast_ema_steps(
    _times: &[i64],
    values: &[f64],
    steps: usize,
    alpha: f64,
) -> Vec<f64> {
    if values.is_empty() {
        return vec![0.0; steps];
    }

    // Compute EMA over the sorted values
    let mut ema = values[0];
    for &v in &values[1..] {
        ema = alpha * v + (1.0 - alpha) * ema;
    }

    // EMA forecast: flat projection at last EMA value
    // With trend: compute EMA of differences for slope
    if values.len() < 3 {
        return vec![ema; steps];
    }

    // Double exponential smoothing (Holt's method) for trend
    let mut level = values[0];
    let mut trend = values[1] - values[0];
    let beta = alpha * 0.5; // trend smoothing factor

    for &v in &values[1..] {
        let prev_level = level;
        level = alpha * v + (1.0 - alpha) * (prev_level + trend);
        trend = beta * (level - prev_level) + (1.0 - beta) * trend;
    }

    (1..=steps)
        .map(|step| level + trend * step as f64)
        .collect()
}

/// Compute MSE for EMA forecast (leave-one-out on last 20% of data).
fn ema_mse(values: &[f64], alpha: f64) -> f64 {
    if values.len() < 4 {
        return f64::MAX;
    }
    let split = values.len() * 4 / 5;
    let train = &values[..split];
    let test = &values[split..];

    // Compute EMA on training data using Holt's method
    let mut level = train[0];
    let mut trend = if train.len() > 1 {
        train[1] - train[0]
    } else {
        0.0
    };
    let beta = alpha * 0.5;
    for &v in &train[1..] {
        let prev_level = level;
        level = alpha * v + (1.0 - alpha) * (prev_level + trend);
        trend = beta * (level - prev_level) + (1.0 - beta) * trend;
    }

    // MSE on test data
    let sse: f64 = test
        .iter()
        .enumerate()
        .map(|(i, &v)| {
            let pred = level + trend * (i + 1) as f64;
            (v - pred).powi(2)
        })
        .sum();
    sse / test.len() as f64
}

/// Forecast N steps ahead using ARIMA(p,d,q).
///
/// Uses the `arima` crate for model fitting (L-BFGS optimization), then performs
/// multi-step forecasting using the fitted AR coefficients on the differenced series.
/// MA residuals are assumed to be 0 for future predictions (standard practice).
fn forecast_arima_steps(
    values: &[f64],
    steps: usize,
    p: usize,
    d: usize,
    q: usize,
) -> Result<Vec<f64>> {
    if values.len() < p + d + 2 {
        return Err(DataFusionError::Execution(format!(
            "forecast_arima: need at least {} data points for ARIMA({p},{d},{q}), got {}",
            p + d + 2,
            values.len()
        )));
    }

    // Fit ARIMA model to get coefficients.
    // The L-BFGS optimizer can panic on degenerate data (e.g. perfectly constant
    // after differencing), so we catch panics and return a graceful error.
    let fit_result = std::panic::catch_unwind(|| arima::estimate::fit(values, p, d, q));
    let coeffs = match fit_result {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            return Err(DataFusionError::Execution(format!(
                "forecast_arima: model fitting failed: {e:?}"
            )));
        }
        Err(_) => {
            return Err(DataFusionError::Execution(
                "forecast_arima: model fitting failed (numerical instability)".into(),
            ));
        }
    };

    // coeffs layout: [intercept, ar1..arp, ma1..maq]
    let intercept = coeffs[0];
    let ar_coeffs: Vec<f64> = if p > 0 {
        coeffs[1..1 + p].to_vec()
    } else {
        vec![]
    };

    // Difference the series d times, keeping last values for un-differencing
    let mut series = values.to_vec();
    let mut last_values_per_level: Vec<f64> = Vec::new();
    for _ in 0..d {
        last_values_per_level.push(*series.last().unwrap_or(&0.0));
        let diffed: Vec<f64> = series.windows(2).map(|w| w[1] - w[0]).collect();
        series = diffed;
    }

    // AR multi-step forecasting on the differenced series
    let mut extended = series;
    for _ in 0..steps {
        let n = extended.len();
        let mut next = intercept;
        for (j, &coeff) in ar_coeffs.iter().enumerate() {
            if n > j {
                next += coeff * extended[n - 1 - j];
            }
        }
        extended.push(next);
    }

    // Extract the forecasted portion (differenced space)
    let forecast_diff: Vec<f64> = extended[extended.len() - steps..].to_vec();

    // Un-difference: integrate back d times
    let mut result = forecast_diff;
    for level in (0..d).rev() {
        let base = last_values_per_level[level];
        let mut integrated = Vec::with_capacity(result.len());
        let mut prev = base;
        for &diff in &result {
            prev += diff;
            integrated.push(prev);
        }
        result = integrated;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// State field helpers (shared across all forecast UDAFs)
// ---------------------------------------------------------------------------

fn forecast_state_fields() -> Vec<Field> {
    vec![
        Field::new("data_json", DataType::Utf8, true),
        Field::new("params_json", DataType::Utf8, true),
    ]
}

/// Merge state from another accumulator's serialized data.
fn merge_ts_state(data: &mut TimeSeriesData, states: &[ArrayRef]) -> Result<()> {
    if states.len() < 2 {
        return Ok(());
    }
    let json_arr = states[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("forecast: state data must be Utf8".into())
        })?;
    for i in 0..json_arr.len() {
        if json_arr.is_null(i) {
            continue;
        }
        let other = TimeSeriesData::from_json(json_arr.value(i))?;
        data.merge(&other);
    }
    Ok(())
}

// ===========================================================================
// forecast_linear(value, time, steps) → Float64
// ===========================================================================

#[derive(Debug, Clone)]
struct ForecastLinearUDAF {
    signature: Signature,
}

impl ForecastLinearUDAF {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ForecastLinearUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "forecast_linear"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn state_fields(
        &self,
        _args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> Result<Vec<Field>> {
        Ok(forecast_state_fields())
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ForecastLinearAccumulator::new()))
    }
}

#[derive(Debug)]
struct ForecastLinearAccumulator {
    data: TimeSeriesData,
    steps: i64,
}

impl ForecastLinearAccumulator {
    fn new() -> Self {
        Self {
            data: TimeSeriesData::new(),
            steps: 1,
        }
    }
}

impl Accumulator for ForecastLinearAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 3 {
            return Err(DataFusionError::Execution(
                "forecast_linear requires 3 arguments: value, time, steps".into(),
            ));
        }
        self.data.ingest(&values[0], &values[1])?;
        self.steps = first_i64_opt(&values[2]).unwrap_or(self.steps);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_ts_state(&mut self.data, states)?;
        // Merge params
        if states.len() >= 2 {
            let params_arr = states[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("forecast: params state must be Utf8".into())
                })?;
            for i in 0..params_arr.len() {
                if !params_arr.is_null(i) {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(params_arr.value(i))
                    {
                        if let Some(s) = parsed.get("steps").and_then(|v| v.as_i64()) {
                            self.steps = s;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let data_json = self.data.to_json();
        let params_json = format!("{{\"steps\":{}}}", self.steps);
        Ok(vec![
            ScalarValue::Utf8(Some(data_json)),
            ScalarValue::Utf8(Some(params_json)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.data.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }
        self.data.sorted();
        let result = forecast_linear_steps(
            &self.data.times,
            &self.data.values,
            self.steps as usize,
        );
        Ok(ScalarValue::Float64(Some(
            *result.last().unwrap_or(&f64::NAN),
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.data.times.capacity() * 8
            + self.data.values.capacity() * 8
    }
}

pub fn create_forecast_linear_udaf() -> AggregateUDF {
    AggregateUDF::from(ForecastLinearUDAF::new())
}

// ===========================================================================
// forecast_ema(value, time, steps, alpha) → Float64
// ===========================================================================

#[derive(Debug, Clone)]
struct ForecastEmaUDAF {
    signature: Signature,
}

impl ForecastEmaUDAF {
    fn new() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ForecastEmaUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "forecast_ema"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn state_fields(
        &self,
        _args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> Result<Vec<Field>> {
        Ok(forecast_state_fields())
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ForecastEmaAccumulator::new()))
    }
}

#[derive(Debug)]
struct ForecastEmaAccumulator {
    data: TimeSeriesData,
    steps: i64,
    alpha: f64,
}

impl ForecastEmaAccumulator {
    fn new() -> Self {
        Self {
            data: TimeSeriesData::new(),
            steps: 1,
            alpha: 0.3,
        }
    }
}

impl Accumulator for ForecastEmaAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 4 {
            return Err(DataFusionError::Execution(
                "forecast_ema requires 4 arguments: value, time, steps, alpha".into(),
            ));
        }
        self.data.ingest(&values[0], &values[1])?;
        self.steps = first_i64_opt(&values[2]).unwrap_or(self.steps);
        self.alpha = first_f64_opt(&values[3]).unwrap_or(self.alpha);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_ts_state(&mut self.data, states)?;
        if states.len() >= 2 {
            let params_arr = states[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("forecast: params state must be Utf8".into())
                })?;
            for i in 0..params_arr.len() {
                if !params_arr.is_null(i) {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(params_arr.value(i))
                    {
                        if let Some(s) = parsed.get("steps").and_then(|v| v.as_i64()) {
                            self.steps = s;
                        }
                        if let Some(a) = parsed.get("alpha").and_then(|v| v.as_f64()) {
                            self.alpha = a;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let data_json = self.data.to_json();
        let params_json = format!("{{\"steps\":{},\"alpha\":{}}}", self.steps, self.alpha);
        Ok(vec![
            ScalarValue::Utf8(Some(data_json)),
            ScalarValue::Utf8(Some(params_json)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.data.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }
        self.data.sorted();
        let result = forecast_ema_steps(
            &self.data.times,
            &self.data.values,
            self.steps as usize,
            self.alpha,
        );
        Ok(ScalarValue::Float64(Some(
            *result.last().unwrap_or(&f64::NAN),
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.data.times.capacity() * 8
            + self.data.values.capacity() * 8
    }
}

pub fn create_forecast_ema_udaf() -> AggregateUDF {
    AggregateUDF::from(ForecastEmaUDAF::new())
}

// ===========================================================================
// forecast_arima(value, time, steps, p, d, q) → Float64
// ===========================================================================

#[derive(Debug, Clone)]
struct ForecastArimaUDAF {
    signature: Signature,
}

impl ForecastArimaUDAF {
    fn new() -> Self {
        Self {
            signature: Signature::any(6, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ForecastArimaUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "forecast_arima"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn state_fields(
        &self,
        _args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> Result<Vec<Field>> {
        Ok(forecast_state_fields())
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ForecastArimaAccumulator::new()))
    }
}

#[derive(Debug)]
struct ForecastArimaAccumulator {
    data: TimeSeriesData,
    steps: i64,
    p: i64,
    d: i64,
    q: i64,
}

impl ForecastArimaAccumulator {
    fn new() -> Self {
        Self {
            data: TimeSeriesData::new(),
            steps: 1,
            p: 1,
            d: 1,
            q: 0,
        }
    }
}

impl Accumulator for ForecastArimaAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 6 {
            return Err(DataFusionError::Execution(
                "forecast_arima requires 6 arguments: value, time, steps, p, d, q".into(),
            ));
        }
        self.data.ingest(&values[0], &values[1])?;
        self.steps = first_i64_opt(&values[2]).unwrap_or(self.steps);
        self.p = first_i64_opt(&values[3]).unwrap_or(self.p);
        self.d = first_i64_opt(&values[4]).unwrap_or(self.d);
        self.q = first_i64_opt(&values[5]).unwrap_or(self.q);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_ts_state(&mut self.data, states)?;
        if states.len() >= 2 {
            let params_arr = states[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("forecast: params state must be Utf8".into())
                })?;
            for i in 0..params_arr.len() {
                if !params_arr.is_null(i) {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(params_arr.value(i))
                    {
                        if let Some(v) = parsed.get("steps").and_then(|v| v.as_i64()) {
                            self.steps = v;
                        }
                        if let Some(v) = parsed.get("p").and_then(|v| v.as_i64()) {
                            self.p = v;
                        }
                        if let Some(v) = parsed.get("d").and_then(|v| v.as_i64()) {
                            self.d = v;
                        }
                        if let Some(v) = parsed.get("q").and_then(|v| v.as_i64()) {
                            self.q = v;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let data_json = self.data.to_json();
        let params_json = format!(
            "{{\"steps\":{},\"p\":{},\"d\":{},\"q\":{}}}",
            self.steps, self.p, self.d, self.q
        );
        Ok(vec![
            ScalarValue::Utf8(Some(data_json)),
            ScalarValue::Utf8(Some(params_json)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.data.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }
        self.data.sorted();
        let forecast = forecast_arima_steps(
            &self.data.values,
            self.steps as usize,
            self.p as usize,
            self.d as usize,
            self.q as usize,
        )?;
        Ok(ScalarValue::Float64(Some(
            *forecast.last().unwrap_or(&f64::NAN),
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.data.times.capacity() * 8
            + self.data.values.capacity() * 8
    }
}

pub fn create_forecast_arima_udaf() -> AggregateUDF {
    AggregateUDF::from(ForecastArimaUDAF::new())
}

// ===========================================================================
// forecast_auto(value, time, steps) → Float64
// ===========================================================================

#[derive(Debug, Clone)]
struct ForecastAutoUDAF {
    signature: Signature,
}

impl ForecastAutoUDAF {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ForecastAutoUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "forecast_auto"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn state_fields(
        &self,
        _args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> Result<Vec<Field>> {
        Ok(forecast_state_fields())
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ForecastAutoAccumulator::new()))
    }
}

#[derive(Debug)]
struct ForecastAutoAccumulator {
    data: TimeSeriesData,
    steps: i64,
}

impl ForecastAutoAccumulator {
    fn new() -> Self {
        Self {
            data: TimeSeriesData::new(),
            steps: 1,
        }
    }
}

impl Accumulator for ForecastAutoAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 3 {
            return Err(DataFusionError::Execution(
                "forecast_auto requires 3 arguments: value, time, steps".into(),
            ));
        }
        self.data.ingest(&values[0], &values[1])?;
        self.steps = first_i64_opt(&values[2]).unwrap_or(self.steps);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_ts_state(&mut self.data, states)?;
        if states.len() >= 2 {
            let params_arr = states[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("forecast: params state must be Utf8".into())
                })?;
            for i in 0..params_arr.len() {
                if !params_arr.is_null(i) {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(params_arr.value(i))
                    {
                        if let Some(s) = parsed.get("steps").and_then(|v| v.as_i64()) {
                            self.steps = s;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let data_json = self.data.to_json();
        let params_json = format!("{{\"steps\":{}}}", self.steps);
        Ok(vec![
            ScalarValue::Utf8(Some(data_json)),
            ScalarValue::Utf8(Some(params_json)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.data.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }
        self.data.sorted();
        let steps = self.steps as usize;

        // Fit linear model
        let linear_result =
            forecast_linear_steps(&self.data.times, &self.data.values, steps);
        let linear_mse_val = linear_mse(&self.data.times, &self.data.values);

        // Fit EMA with alpha=0.3
        let ema_result = forecast_ema_steps(
            &self.data.times,
            &self.data.values,
            steps,
            0.3,
        );
        let ema_mse_val = ema_mse(&self.data.values, 0.3);

        // Pick the model with lower MSE
        let result = if ema_mse_val < linear_mse_val {
            &ema_result
        } else {
            &linear_result
        };

        Ok(ScalarValue::Float64(Some(
            *result.last().unwrap_or(&f64::NAN),
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.data.times.capacity() * 8
            + self.data.values.capacity() * 8
    }
}

pub fn create_forecast_auto_udaf() -> AggregateUDF {
    AggregateUDF::from(ForecastAutoUDAF::new())
}

// ===========================================================================
// forecast_series(value, time, steps, model) → Utf8 (JSON)
// ===========================================================================

#[derive(Debug, Clone)]
struct ForecastSeriesUDAF {
    signature: Signature,
}

impl ForecastSeriesUDAF {
    fn new() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ForecastSeriesUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "forecast_series"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn state_fields(
        &self,
        _args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> Result<Vec<Field>> {
        Ok(forecast_state_fields())
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ForecastSeriesAccumulator::new()))
    }
}

#[derive(Debug)]
struct ForecastSeriesAccumulator {
    data: TimeSeriesData,
    steps: i64,
    model: String,
}

impl ForecastSeriesAccumulator {
    fn new() -> Self {
        Self {
            data: TimeSeriesData::new(),
            steps: 5,
            model: "linear".to_string(),
        }
    }
}

impl Accumulator for ForecastSeriesAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 4 {
            return Err(DataFusionError::Execution(
                "forecast_series requires 4 arguments: value, time, steps, model".into(),
            ));
        }
        self.data.ingest(&values[0], &values[1])?;
        self.steps = first_i64_opt(&values[2]).unwrap_or(self.steps);
        if let Some(m) = first_string_opt(&values[3]) {
            self.model = m;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_ts_state(&mut self.data, states)?;
        if states.len() >= 2 {
            let params_arr = states[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("forecast: params state must be Utf8".into())
                })?;
            for i in 0..params_arr.len() {
                if !params_arr.is_null(i) {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(params_arr.value(i))
                    {
                        if let Some(s) = parsed.get("steps").and_then(|v| v.as_i64()) {
                            self.steps = s;
                        }
                        if let Some(m) = parsed.get("model").and_then(|v| v.as_str()) {
                            self.model = m.to_string();
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let data_json = self.data.to_json();
        let params_json = format!(
            "{{\"steps\":{},\"model\":\"{}\"}}",
            self.steps, self.model
        );
        Ok(vec![
            ScalarValue::Utf8(Some(data_json)),
            ScalarValue::Utf8(Some(params_json)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.data.is_empty() {
            return Ok(ScalarValue::Utf8(None));
        }
        self.data.sorted();
        let steps = self.steps as usize;
        let n = self.data.len();

        // Compute average time interval for generating future timestamps
        let avg_interval = if n > 1 {
            (self.data.times[n - 1] - self.data.times[0]) as f64 / (n - 1) as f64
        } else {
            1.0
        };

        // Run the selected model
        let predictions = match self.model.as_str() {
            "ema" => forecast_ema_steps(
                &self.data.times,
                &self.data.values,
                steps,
                0.3,
            ),
            "arima" => forecast_arima_steps(&self.data.values, steps, 1, 1, 0)
                .unwrap_or_else(|_| {
                    forecast_linear_steps(&self.data.times, &self.data.values, steps)
                }),
            _ => forecast_linear_steps(&self.data.times, &self.data.values, steps),
        };

        // Build JSON array: [{"step":1,"time":..., "value":...}, ...]
        let last_time = self.data.times[n - 1];
        let entries: Vec<String> = predictions
            .iter()
            .enumerate()
            .map(|(i, &v)| {
                let future_time = last_time as f64 + avg_interval * (i + 1) as f64;
                format!(
                    "{{\"step\":{},\"time\":{},\"value\":{:.6}}}",
                    i + 1,
                    future_time as i64,
                    v
                )
            })
            .collect();
        let json = format!("[{}]", entries.join(","));

        Ok(ScalarValue::Utf8(Some(json)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.data.times.capacity() * 8
            + self.data.values.capacity() * 8
            + self.model.capacity()
    }
}

pub fn create_forecast_series_udaf() -> AggregateUDF {
    AggregateUDF::from(ForecastSeriesUDAF::new())
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::prelude::*;
    use std::sync::Arc;

    // Helper: create a SessionContext with all forecast UDAFs registered.
    fn ctx_with_forecasts() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udaf(create_forecast_linear_udaf());
        ctx.register_udaf(create_forecast_ema_udaf());
        ctx.register_udaf(create_forecast_arima_udaf());
        ctx.register_udaf(create_forecast_auto_udaf());
        ctx.register_udaf(create_forecast_series_udaf());
        ctx
    }

    // Helper: register a simple linear time series (y = 2*x + 10).
    fn register_linear_data(ctx: &SessionContext) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Float64, false),
            Field::new("time", DataType::Int64, false),
        ]));
        let values: Vec<f64> = (0..20).map(|i| 2.0 * i as f64 + 10.0).collect();
        let times: Vec<i64> = (0..20).map(|i| 1000 + i * 100).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(values)),
                Arc::new(Int64Array::from(times)),
            ],
        )
        .unwrap();
        ctx.register_batch("metrics", batch).unwrap();
    }

    // Helper: register grouped time series data.
    fn register_grouped_data(ctx: &SessionContext) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("time", DataType::Int64, false),
        ]));
        // Host A: linear y = 3x + 5
        // Host B: linear y = -x + 100
        let mut hosts = Vec::new();
        let mut values = Vec::new();
        let mut times = Vec::new();
        for i in 0..10 {
            hosts.push("a");
            values.push(3.0 * i as f64 + 5.0);
            times.push(1000_i64 + i * 100);
        }
        for i in 0..10 {
            hosts.push("b");
            values.push(-1.0 * i as f64 + 100.0);
            times.push(1000_i64 + i * 100);
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(hosts)),
                Arc::new(Float64Array::from(values)),
                Arc::new(Int64Array::from(times)),
            ],
        )
        .unwrap();
        ctx.register_batch("grouped_metrics", batch).unwrap();
    }

    // -----------------------------------------------------------------------
    // forecast_linear tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_forecast_linear_basic() {
        let ctx = ctx_with_forecasts();
        register_linear_data(&ctx);

        let df = ctx
            .sql("SELECT forecast_linear(value, time, 1) as pred FROM metrics")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // y = 2x + 10, last point at x=19 → y=48
        // Next step at x=20 → y=50
        let pred = arr.value(0);
        assert!(
            (pred - 50.0).abs() < 1.0,
            "expected ~50.0, got {pred}"
        );
    }

    #[tokio::test]
    async fn test_forecast_linear_multiple_steps() {
        let ctx = ctx_with_forecasts();
        register_linear_data(&ctx);

        let df = ctx
            .sql("SELECT forecast_linear(value, time, 5) as pred FROM metrics")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // 5 steps ahead: x=24 → y=58
        let pred = arr.value(0);
        assert!(
            (pred - 58.0).abs() < 1.0,
            "expected ~58.0, got {pred}"
        );
    }

    #[tokio::test]
    async fn test_forecast_linear_grouped() {
        let ctx = ctx_with_forecasts();
        register_grouped_data(&ctx);

        let df = ctx
            .sql(
                "SELECT host, forecast_linear(value, time, 1) as pred \
                 FROM grouped_metrics GROUP BY host ORDER BY host",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let preds = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Host A: y=3x+5, last x=9 (y=32), next x=10 → y=35
        assert!(
            (preds.value(0) - 35.0).abs() < 1.0,
            "host a: expected ~35, got {}",
            preds.value(0)
        );
        // Host B: y=-x+100, last x=9 (y=91), next x=10 → y=90
        assert!(
            (preds.value(1) - 90.0).abs() < 1.0,
            "host b: expected ~90, got {}",
            preds.value(1)
        );
    }

    #[tokio::test]
    async fn test_forecast_linear_single_value() {
        let ctx = ctx_with_forecasts();
        // Single data point should return that value as the forecast
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Float64, false),
            Field::new("time", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(vec![42.0])),
                Arc::new(Int64Array::from(vec![1000])),
            ],
        )
        .unwrap();
        ctx.register_batch("single_metric", batch).unwrap();

        let df = ctx
            .sql("SELECT forecast_linear(value, time, 1) as pred FROM single_metric")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 42.0);
    }

    // -----------------------------------------------------------------------
    // forecast_ema tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_forecast_ema_trending() {
        let ctx = ctx_with_forecasts();
        register_linear_data(&ctx);

        let df = ctx
            .sql("SELECT forecast_ema(value, time, 1, 0.3) as pred FROM metrics")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let pred = arr.value(0);
        // EMA on a linear trend should predict close to the next value
        // The Holt's method should capture the trend
        assert!(
            pred > 45.0 && pred < 55.0,
            "expected ~48-52, got {pred}"
        );
    }

    #[tokio::test]
    async fn test_forecast_ema_high_alpha() {
        let ctx = ctx_with_forecasts();
        register_linear_data(&ctx);

        // High alpha = more responsive to recent data
        let df = ctx
            .sql("SELECT forecast_ema(value, time, 1, 0.9) as pred FROM metrics")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let pred = arr.value(0);
        assert!(
            pred > 45.0 && pred < 55.0,
            "expected ~48-52, got {pred}"
        );
    }

    // -----------------------------------------------------------------------
    // forecast_arima tests
    // -----------------------------------------------------------------------

    // Helper: register noisy linear time series (y = 2*x + 10 + noise).
    fn register_noisy_data(ctx: &SessionContext) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Float64, false),
            Field::new("time", DataType::Int64, false),
        ]));
        // Add small noise to avoid perfectly constant differenced series
        let noise = [0.1, -0.2, 0.3, -0.1, 0.2, -0.3, 0.15, -0.15, 0.25, -0.05,
                     0.12, -0.22, 0.18, -0.08, 0.28, -0.18, 0.05, -0.25, 0.22, -0.12];
        let values: Vec<f64> = (0..20)
            .map(|i| 2.0 * i as f64 + 10.0 + noise[i])
            .collect();
        let times: Vec<i64> = (0..20).map(|i| 1000 + i as i64 * 100).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(values)),
                Arc::new(Int64Array::from(times)),
            ],
        )
        .unwrap();
        ctx.register_batch("noisy_metrics", batch).unwrap();
    }

    #[tokio::test]
    async fn test_forecast_arima_basic() {
        let ctx = ctx_with_forecasts();
        register_noisy_data(&ctx);

        // ARIMA(1,1,0) on noisy linear data
        let df = ctx
            .sql("SELECT forecast_arima(value, time, 1, 1, 1, 0) as pred FROM noisy_metrics")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let pred = arr.value(0);
        // ARIMA(1,1,0) on ~linear data should predict close to 50
        assert!(
            pred > 40.0 && pred < 60.0,
            "expected ~50, got {pred}"
        );
    }

    #[tokio::test]
    async fn test_forecast_arima_multi_step() {
        let ctx = ctx_with_forecasts();
        register_noisy_data(&ctx);

        let df = ctx
            .sql("SELECT forecast_arima(value, time, 3, 1, 1, 0) as pred FROM noisy_metrics")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let pred = arr.value(0);
        // 3 steps ahead on ~linear data → ~54
        assert!(
            pred > 44.0 && pred < 64.0,
            "expected ~54, got {pred}"
        );
    }

    // -----------------------------------------------------------------------
    // forecast_auto tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_forecast_auto_linear_data() {
        let ctx = ctx_with_forecasts();
        register_linear_data(&ctx);

        let df = ctx
            .sql("SELECT forecast_auto(value, time, 1) as pred FROM metrics")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let pred = arr.value(0);
        // Auto should pick linear regression for perfectly linear data
        assert!(
            (pred - 50.0).abs() < 3.0,
            "expected ~50, got {pred}"
        );
    }

    #[tokio::test]
    async fn test_forecast_auto_grouped() {
        let ctx = ctx_with_forecasts();
        register_grouped_data(&ctx);

        let df = ctx
            .sql(
                "SELECT host, forecast_auto(value, time, 1) as pred \
                 FROM grouped_metrics GROUP BY host ORDER BY host",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let preds = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Host A trend up, Host B trend down
        assert!(preds.value(0) > 30.0, "host a should trend up");
        assert!(preds.value(1) < 95.0, "host b should trend down");
    }

    // -----------------------------------------------------------------------
    // forecast_series tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_forecast_series_linear() {
        let ctx = ctx_with_forecasts();
        register_linear_data(&ctx);

        let df = ctx
            .sql("SELECT forecast_series(value, time, 3, 'linear') as pred FROM metrics")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let json_str = arr.value(0);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(json_str).unwrap();
        assert_eq!(parsed.len(), 3, "should have 3 forecast steps");

        // Check each step has expected fields
        for (i, entry) in parsed.iter().enumerate() {
            assert_eq!(entry["step"].as_i64().unwrap(), (i + 1) as i64);
            assert!(entry["time"].as_i64().is_some());
            assert!(entry["value"].as_f64().is_some());
        }

        // First step value should be close to 50
        let step1_value = parsed[0]["value"].as_f64().unwrap();
        assert!(
            (step1_value - 50.0).abs() < 1.0,
            "step 1 expected ~50, got {step1_value}"
        );
    }

    #[tokio::test]
    async fn test_forecast_series_ema() {
        let ctx = ctx_with_forecasts();
        register_linear_data(&ctx);

        let df = ctx
            .sql("SELECT forecast_series(value, time, 3, 'ema') as pred FROM metrics")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let json_str = arr.value(0);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(json_str).unwrap();
        assert_eq!(parsed.len(), 3);

        // Values should be increasing (trending data)
        let v1 = parsed[0]["value"].as_f64().unwrap();
        let v2 = parsed[1]["value"].as_f64().unwrap();
        let v3 = parsed[2]["value"].as_f64().unwrap();
        assert!(v2 > v1, "step 2 ({v2}) should be > step 1 ({v1})");
        assert!(v3 > v2, "step 3 ({v3}) should be > step 2 ({v2})");
    }

    #[tokio::test]
    async fn test_forecast_series_arima() {
        let ctx = ctx_with_forecasts();
        register_noisy_data(&ctx);

        let df = ctx
            .sql("SELECT forecast_series(value, time, 3, 'arima') as pred FROM noisy_metrics")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let json_str = arr.value(0);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(json_str).unwrap();
        assert_eq!(parsed.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Model function unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_fit_linear_perfect() {
        // Perfect linear data: y = 2x + 10
        let times: Vec<i64> = (0..10).map(|i| i * 100).collect();
        let values: Vec<f64> = (0..10).map(|i| 2.0 * i as f64 + 10.0).collect();

        let predictions = forecast_linear_steps(&times, &values, 1);
        // Next step: x=10 → y=30
        assert!(
            (predictions[0] - 30.0).abs() < 0.1,
            "expected 30.0, got {}",
            predictions[0]
        );
    }

    #[test]
    fn test_fit_linear_single_point() {
        let times = vec![100];
        let values = vec![42.0];
        let predictions = forecast_linear_steps(&times, &values, 1);
        assert_eq!(predictions[0], 42.0);
    }

    #[test]
    fn test_forecast_ema_steps_flat() {
        // Flat data should predict the same value
        let times: Vec<i64> = (0..10).map(|i| i * 100).collect();
        let values = vec![5.0; 10];
        let predictions = forecast_ema_steps(&times, &values, 3, 0.3);
        for &p in &predictions {
            assert!(
                (p - 5.0).abs() < 0.5,
                "flat data should predict ~5.0, got {p}"
            );
        }
    }

    #[test]
    fn test_arima_forecast_noisy_trend() {
        // Noisy linear data: y = 2x + 10 + noise
        let noise = [0.1, -0.2, 0.3, -0.1, 0.2, -0.3, 0.15, -0.15, 0.25, -0.05,
                     0.12, -0.22, 0.18, -0.08, 0.28, -0.18, 0.05, -0.25, 0.22, -0.12];
        let values: Vec<f64> = (0..20)
            .map(|i| 2.0 * i as f64 + 10.0 + noise[i])
            .collect();
        let forecast = forecast_arima_steps(&values, 1, 1, 1, 0).unwrap();
        assert_eq!(forecast.len(), 1);
        // Should predict close to 50 (next value after y≈48 at x=19)
        assert!(
            (forecast[0] - 50.0).abs() < 5.0,
            "expected ~50, got {}",
            forecast[0]
        );
    }

    #[test]
    fn test_arima_too_few_points() {
        let values = vec![1.0, 2.0];
        let result = forecast_arima_steps(&values, 1, 2, 1, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_time_series_data_json_roundtrip() {
        let mut data = TimeSeriesData::new();
        data.times = vec![100, 200, 300];
        data.values = vec![1.5, 2.5, 3.5];

        let json = data.to_json();
        let restored = TimeSeriesData::from_json(&json).unwrap();
        assert_eq!(restored.times, data.times);
        assert_eq!(restored.values, data.values);
    }

    #[test]
    fn test_time_series_data_merge() {
        let mut a = TimeSeriesData::new();
        a.times = vec![100, 300];
        a.values = vec![1.0, 3.0];

        let b = TimeSeriesData {
            times: vec![200],
            values: vec![2.0],
        };
        a.merge(&b);
        a.sorted();

        assert_eq!(a.times, vec![100, 200, 300]);
        assert_eq!(a.values, vec![1.0, 2.0, 3.0]);
    }
}
