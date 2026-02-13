# Time-Series Forecasting Guide

MynaDB includes built-in time-series forecasting as SQL aggregate functions. Predict future values directly in your queries without external tools.

## Functions

| Function | Model | Returns | Description |
|----------|-------|---------|-------------|
| `forecast_linear(value, time, steps)` | OLS Linear Regression | Float64 | Trend-based prediction |
| `forecast_ema(value, time, steps, alpha)` | Exponential Moving Average (Holt's) | Float64 | Smoothed prediction with trend |
| `forecast_arima(value, time, steps, p, d, q)` | ARIMA(p,d,q) | Float64 | Auto-regressive integrated moving average |
| `forecast_auto(value, time, steps)` | Best of Linear/EMA | Float64 | Automatically picks the best model |
| `forecast_series(value, time, steps, model)` | Any | Utf8 (JSON) | Multi-step forecast as JSON array |

### Arguments

- **value** (Float64) - The metric column to forecast
- **time** (Int64 or Timestamp) - The time column (nanoseconds or any timestamp type)
- **steps** (Int64) - Number of time intervals to forecast ahead
- **alpha** (Float64) - EMA smoothing factor, 0 < alpha < 1 (higher = more responsive to recent data)
- **p, d, q** (Int64) - ARIMA order parameters: AR order, differencing order, MA order
- **model** (Utf8) - Model name for `forecast_series`: `'linear'`, `'ema'`, or `'arima'`

## Quick Start

```sql
-- Predict CPU usage 3 steps ahead using linear regression
SELECT host,
       forecast_linear(cpu_usage, time, 3) as predicted_cpu
FROM metrics
WHERE time > now() - interval '1 hour'
GROUP BY host;

-- Auto-select the best model
SELECT sensor_id,
       forecast_auto(temperature, time, 1) as next_temp
FROM sensors
GROUP BY sensor_id;
```

## Examples

### Linear Regression

Best for data with a clear upward or downward trend.

```sql
-- Predict next value for each host
SELECT host,
       forecast_linear(cpu_usage, time, 1) as next_cpu
FROM metrics
WHERE time > '2024-01-01'
GROUP BY host;

-- Predict 5 steps ahead
SELECT forecast_linear(memory_used, time, 5) as predicted_mem
FROM system_metrics;
```

### Exponential Moving Average

Best for noisy data where recent values matter more. Uses Holt's double exponential smoothing to capture trends.

```sql
-- High alpha (0.8) = reactive to recent changes
SELECT host,
       forecast_ema(response_time, time, 1, 0.8) as predicted_rt
FROM api_metrics
GROUP BY host;

-- Low alpha (0.1) = smooth, stable predictions
SELECT forecast_ema(temperature, time, 3, 0.1) as predicted_temp
FROM weather_data;
```

### ARIMA

Best for stationary or differenced-stationary time series. Common configurations:

| ARIMA(p,d,q) | Use Case |
|--------------|----------|
| ARIMA(1,1,0) | Random walk with drift - general purpose |
| ARIMA(1,0,0) | AR(1) - stationary data with autocorrelation |
| ARIMA(2,1,0) | Two-lag autoregressive with differencing |
| ARIMA(1,1,1) | General ARIMA - combines AR and MA |

```sql
-- ARIMA(1,1,0) - most common for trending time series
SELECT host,
       forecast_arima(cpu_usage, time, 1, 1, 1, 0) as predicted_cpu
FROM metrics
WHERE time > now() - interval '6 hours'
GROUP BY host;

-- ARIMA(2,1,0) for more complex patterns
SELECT forecast_arima(value, time, 3, 2, 1, 0) as predicted
FROM sensor_data;
```

**Requirements**: ARIMA needs at least `p + d + 2` data points per group. If the data is too regular (perfectly linear), the model may fall back to a simpler prediction.

### Auto Model Selection

Fits both linear regression and EMA (alpha=0.3), picks whichever has lower mean squared error on the training data.

```sql
SELECT host,
       forecast_auto(latency, time, 1) as predicted_latency
FROM network_metrics
WHERE time > now() - interval '30 minutes'
GROUP BY host;
```

### Multi-Step Forecast (JSON)

Returns all forecast steps as a JSON array, useful for charting or downstream processing.

```sql
SELECT host,
       forecast_series(cpu_usage, time, 5, 'linear') as forecast_json
FROM metrics
GROUP BY host;
-- Returns: [{"step":1,"time":1707350400,"value":42.1},{"step":2,...},...]

-- Using EMA model
SELECT forecast_series(temperature, time, 10, 'ema') as forecast
FROM weather GROUP BY station;

-- Using ARIMA (defaults to ARIMA(1,1,0))
SELECT forecast_series(value, time, 5, 'arima') as forecast
FROM sensors GROUP BY sensor_id;
```

**JSON output format**:
```json
[
  {"step": 1, "time": 1707350400, "value": 42.100000},
  {"step": 2, "time": 1707354000, "value": 43.200000},
  {"step": 3, "time": 1707357600, "value": 44.300000}
]
```

## How It Works

All forecasting functions are **aggregate functions** (UDAFs). They:

1. **Collect** all `(time, value)` pairs for each GROUP BY group
2. **Sort** by time
3. **Fit** the selected model on the sorted data
4. **Predict** N steps ahead using the estimated average time interval

The `steps` parameter predicts relative to the **last data point**. For example, if your data ends at 10:00 with 1-minute intervals, `steps=3` predicts for 10:01, 10:02, and 10:03. Single-step functions return the value at the final step.

### State Merging

For distributed execution, accumulators serialize their collected time-series data as JSON and merge across partitions before final evaluation. This ensures correct results even when data is split across multiple execution nodes.

## Model Selection Guide

| Data Pattern | Recommended Function | Why |
|-------------|---------------------|-----|
| Clear linear trend | `forecast_linear` | Exact fit, fast |
| Noisy with trend | `forecast_ema` (alpha=0.3-0.5) | Smooths noise, captures trend |
| Noisy, no clear trend | `forecast_ema` (alpha=0.1) | Heavy smoothing |
| Stationary oscillating | `forecast_arima(v, t, n, 1, 0, 0)` | AR(1) captures autocorrelation |
| Trending + autocorrelated | `forecast_arima(v, t, n, 1, 1, 0)` | Differencing removes trend |
| Unknown pattern | `forecast_auto` | Tries multiple, picks best |
| Need multiple predictions | `forecast_series` | JSON array output |

## Limitations

- **Memory**: All data points for each group are held in memory during aggregation. For very large groups (millions of points), consider pre-aggregating with `time_bucket` first.
- **ARIMA stability**: The L-BFGS optimizer may fail on degenerate data (e.g., perfectly constant series). In such cases, the function returns a graceful error.
- **Seasonality**: The current models do not handle explicit seasonality. For seasonal data, pre-process with `time_bucket` aggregation or use ARIMA with appropriate differencing.
- **Point forecasts**: Single-step functions return point predictions without confidence intervals. Use `forecast_series` for multi-step analysis.
