//! Predicate Evaluation for Cache Filter Pushdown
//!
//! Evaluates DataFusion expressions (predicates) on RecordBatches to filter
//! data early at the cache level, reducing memory transfer.

use datafusion::arrow::array::{Array, BooleanArray, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalExpr;
use std::sync::Arc;

/// Apply a predicate (filter) to a RecordBatch
///
/// Returns a new RecordBatch containing only rows that match the predicate.
/// If predicate evaluation fails, returns the original batch unfiltered.
pub fn filter_batch(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> DataFusionResult<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    // Evaluate predicate to get boolean array
    let filter_array = predicate.evaluate(batch)?.into_array(batch.num_rows())?;

    // Downcast to BooleanArray
    let boolean_array = filter_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Predicate did not evaluate to boolean array".to_string())
        })?;

    // Apply filter to batch
    let filtered = filter_record_batch(batch, boolean_array)?;

    Ok(filtered)
}

/// Apply multiple predicates (AND semantics) to a RecordBatch
///
/// Returns a new RecordBatch containing only rows that match ALL predicates.
pub fn filter_batch_with_predicates(
    batch: &RecordBatch,
    predicates: &[Arc<dyn PhysicalExpr>],
) -> DataFusionResult<RecordBatch> {
    if predicates.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let mut filtered_batch = batch.clone();

    // Apply predicates sequentially (short-circuit on empty result)
    for predicate in predicates {
        if filtered_batch.num_rows() == 0 {
            break;
        }
        filtered_batch = filter_batch(&filtered_batch, predicate)?;
    }

    Ok(filtered_batch)
}

/// Apply predicate to multiple batches, filtering each one
///
/// Returns only non-empty batches after filtering.
pub fn filter_batches(
    batches: &[RecordBatch],
    predicate: &Arc<dyn PhysicalExpr>,
) -> DataFusionResult<Vec<RecordBatch>> {
    let mut result = Vec::with_capacity(batches.len());

    for batch in batches {
        let filtered = filter_batch(batch, predicate)?;
        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }

    Ok(result)
}

/// Apply multiple predicates to multiple batches
pub fn filter_batches_with_predicates(
    batches: &[RecordBatch],
    predicates: &[Arc<dyn PhysicalExpr>],
) -> DataFusionResult<Vec<RecordBatch>> {
    if predicates.is_empty() {
        return Ok(batches.to_vec());
    }

    let mut result = Vec::with_capacity(batches.len());

    for batch in batches {
        let filtered = filter_batch_with_predicates(batch, predicates)?;
        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::expressions::lit;
    use datafusion::prelude::Expr;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::logical_expr::Operator;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("temperature", DataType::Float64, false),
            Field::new("location", DataType::Utf8, false),
        ]));

        let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let temps = Float64Array::from(vec![70.0, 85.0, 90.0, 75.0, 95.0]);
        let locations = StringArray::from(vec!["NYC", "LA", "NYC", "LA", "NYC"]);

        RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(temps), Arc::new(locations)],
        )
        .unwrap()
    }

    #[test]
    fn test_filter_batch_simple() {
        let batch = create_test_batch();
        let schema = batch.schema();

        // Create predicate: temperature > 80
        let temp_col = col("temperature", &schema).unwrap();
        let threshold = Arc::new(datafusion::physical_expr::expressions::Literal::new(
            datafusion::scalar::ScalarValue::Float64(Some(80.0)),
        ));

        let predicate = Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
            temp_col,
            datafusion::logical_expr::Operator::Gt,
            threshold,
        )) as Arc<dyn PhysicalExpr>;

        let filtered = filter_batch(&batch, &predicate).unwrap();

        // Should have 3 rows (temps: 85, 90, 95)
        assert_eq!(filtered.num_rows(), 3);

        let temps = filtered
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(temps.value(0), 85.0);
        assert_eq!(temps.value(1), 90.0);
        assert_eq!(temps.value(2), 95.0);
    }

    #[test]
    fn test_filter_batch_string_eq() {
        let batch = create_test_batch();
        let schema = batch.schema();

        // Create predicate: location = 'NYC'
        let loc_col = col("location", &schema).unwrap();
        let nyc_lit = Arc::new(datafusion::physical_expr::expressions::Literal::new(
            datafusion::scalar::ScalarValue::Utf8(Some("NYC".to_string())),
        ));

        let predicate = Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
            loc_col,
            datafusion::logical_expr::Operator::Eq,
            nyc_lit,
        )) as Arc<dyn PhysicalExpr>;

        let filtered = filter_batch(&batch, &predicate).unwrap();

        // Should have 3 rows (ids: 1, 3, 5)
        assert_eq!(filtered.num_rows(), 3);

        let ids = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 3);
        assert_eq!(ids.value(2), 5);
    }

    #[test]
    fn test_filter_batches() {
        let batch = create_test_batch();
        let schema = batch.schema();

        // Create predicate: temperature > 80
        let temp_col = col("temperature", &schema).unwrap();
        let threshold = Arc::new(datafusion::physical_expr::expressions::Literal::new(
            datafusion::scalar::ScalarValue::Float64(Some(80.0)),
        ));

        let predicate = Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
            temp_col,
            datafusion::logical_expr::Operator::Gt,
            threshold,
        )) as Arc<dyn PhysicalExpr>;

        let batches = vec![batch.clone(), batch.clone()];
        let filtered = filter_batches(&batches, &predicate).unwrap();

        // Should have 2 batches, each with 3 rows
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].num_rows(), 3);
        assert_eq!(filtered[1].num_rows(), 3);
    }
}
