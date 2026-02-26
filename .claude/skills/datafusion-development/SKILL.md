---
name: datafusion-development
description: This skill should be used when the user is working with Apache DataFusion — adding UDFs, UDAFs, custom TableProviders, analyzer rules, logical optimizer rules, physical optimizer rules, or configuring the SessionContext. Also use when debugging query execution, explain plans, or DataFusion-related errors.
version: 1.0.0
---

# Apache DataFusion Development in MynaDB

## DataFusion Version

MynaDB uses DataFusion as the core query engine. Check `Cargo.toml` for the exact version. Key crates: `datafusion`, `datafusion-common`, `datafusion-expr`, `datafusion-physical-expr`.

## SessionContext Configuration

**File:** `src/query/query_engine/mod.rs`

MynaDB creates a customized `SessionContext` with:
- Target partitions = CPU cores
- Batch size = 8192
- Parquet pushdown filters enabled
- Bloom filter pruning enabled
- Row group / page-level pruning enabled
- Memory limit: 16GB default, 80% spill threshold
- Round-robin repartition disabled (time-series optimization)
- Top-K optimization enabled

## Adding a Scalar UDF

**Location:** `src/query/functions/`

```rust
use datafusion::logical_expr::{ScalarUDFImpl, Signature, Volatility, TypeSignature};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::array::ArrayRef;
use datafusion::common::Result;

#[derive(Debug)]
pub struct MyFunction {
    signature: Signature,
}

impl MyFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MyFunction {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "my_function" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        // Implementation here
        // args[0] is first argument, args[1] is second, etc.
        todo!()
    }
}
```

Register in `src/query/functions/mod.rs`:
```rust
ctx.register_udf(ScalarUDF::from(MyFunction::new()));
```

**Existing examples:**
- `time_bucket.rs` — interval-based time bucketing
- `geospatial.rs` — geo_distance, geo_hash, geo_within
- `forecasting.rs` — forecast_linear, forecast_ema, forecast_arima

## Adding a UDAF (Aggregate)

```rust
use datafusion::logical_expr::{AggregateUDFImpl, Signature, Volatility};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::Accumulator;

#[derive(Debug)]
pub struct MyAggregate { signature: Signature }

impl AggregateUDFImpl for MyAggregate {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "my_aggregate" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MyAccumulator::new()))
    }
}

struct MyAccumulator { /* state */ }

impl Accumulator for MyAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> { todo!() }
    fn evaluate(&mut self) -> Result<ScalarValue> { todo!() }
    fn state(&mut self) -> Result<Vec<ScalarValue>> { todo!() }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> { todo!() }
    fn size(&self) -> usize { std::mem::size_of::<Self>() }
}
```

**Existing examples:** `first_last.rs` — first_value/last_value ordered by time.

## Adding an Analyzer Rule

**Location:** `src/query/analyzers/`

Analyzer rules run before optimization. They rewrite the logical plan for semantic purposes.

```rust
use datafusion::optimizer::AnalyzerRule;
use datafusion::logical_expr::LogicalPlan;
use datafusion::config::ConfigOptions;

pub struct MyAnalyzerRule;

impl AnalyzerRule for MyAnalyzerRule {
    fn name(&self) -> &str { "my_analyzer_rule" }
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        // Walk the plan tree, rewrite nodes as needed
        plan.transform_up(|node| {
            // Return Transformed::yes(new_node) if modified
            // Return Transformed::no(node) if unchanged
            Ok(Transformed::no(node))
        }).map(|t| t.data)
    }
}
```

Register: `ctx.add_analyzer_rule(Arc::new(MyAnalyzerRule));`

## Adding a Logical Optimizer Rule

**Location:** `src/query/logical_optimizers/`

```rust
use datafusion::optimizer::OptimizerRule;
use datafusion::logical_expr::LogicalPlan;

pub struct MyOptimizerRule;

impl OptimizerRule for MyOptimizerRule {
    fn name(&self) -> &str { "my_optimizer_rule" }
    fn try_optimize(&self, plan: &LogicalPlan, config: &dyn OptimizerConfig)
        -> Result<Option<LogicalPlan>>
    {
        // Return Some(new_plan) if optimized, None if no change
        Ok(None)
    }
}
```

## Adding a Custom TableProvider

**Location:** `src/query/providers/`

```rust
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;

pub struct MyTableProvider { schema: SchemaRef }

#[async_trait]
impl TableProvider for MyTableProvider {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Build execution plan
        // Use filters for predicate pushdown
        // Use projection for column pruning
        // Use limit for top-K optimization
        todo!()
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr])
        -> Result<Vec<TableProviderFilterPushDown>>
    {
        // Return Exact, Inexact, or Unsupported per filter
        Ok(filters.iter().map(|_| TableProviderFilterPushDown::Inexact).collect())
    }
}
```

**Existing providers:**
- `table_provider.rs` — ParquetTableProvider (reads Parquet from object store)
- `memtable_provider.rs` — In-memory RecordBatch provider (unflushed cache data)
- `union_provider.rs` — Combines Parquet + memtable for unified view

## Debugging Query Execution

1. Use `/explain/:db?sql=...&verbose=true` for logical + physical plan
2. Use `/analyze/:db?sql=...` for partition/file counts and applied optimizations
3. Check `src/observability/query_tracer.rs` for per-phase timing
4. Check `src/observability/slow_query_logger.rs` for slow query details

## Common Patterns

- **Predicate extraction:** Use `datafusion::physical_optimizer::pruning` for row group pruning
- **Schema merging:** Convert all batches to unified schema before processing
- **Dictionary handling:** Normalize `Dictionary(Int32, Utf8)` to plain `Utf8`
- **Timestamp handling:** MynaDB uses `TimestampNanosecond` internally, convert at boundaries
- **Date partition:** `Date32` = days since Unix epoch, used for partition pruning
