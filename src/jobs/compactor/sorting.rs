//! Sorting Operations for Compactor
//!
//! Handles cardinality-based sorting for optimal data locality.

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn};
use arrow::datatypes::{DataType, Schema};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::debug;

use super::{Compactor, CARDINALITY_SAMPLE_SIZE, HIGH_CARDINALITY_THRESHOLD};

impl Compactor {
    /// Merge and sort batches with cardinality-based tag ordering
    /// Sort order: [low cardinality tags] -> [high cardinality tags] -> time
    /// This preserves optimal data locality for query filtering
    pub(crate) fn merge_sorted_batches(
        batches: Vec<RecordBatch>,
        schema: &Arc<Schema>,
    ) -> Result<RecordBatch, String> {
        if batches.is_empty() {
            return Err("No batches to merge".to_string());
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        // First, concatenate all batches
        let merged = concat_batches(schema, &batches)
            .map_err(|e| format!("Failed to concat batches: {}", e))?;

        // Check if we have a time column
        if schema.index_of("time").is_err() {
            debug!("No time column found, returning concatenated batch without sort");
            return Ok(merged);
        }

        // Analyze cardinality and build optimal sort order
        let sort_order = Self::build_cardinality_sort_order(&merged, schema);

        if sort_order.is_empty() {
            debug!("No sortable columns found");
            return Ok(merged);
        }

        // Log the sort order being used
        debug!("Compaction sort order: {}", sort_order.join(" -> "));

        // Sort using lexicographic multi-column sort
        Self::sort_batch_by_columns(&merged, &sort_order)
    }

    /// Analyze cardinality of string columns and build optimal sort order
    /// Returns column names sorted by cardinality (low to high), with time at the end
    pub(crate) fn build_cardinality_sort_order(
        batch: &RecordBatch,
        schema: &Arc<Schema>,
    ) -> Vec<String> {
        let mut tag_cardinalities: Vec<(String, usize)> = Vec::new();

        // Find string columns (tags) and estimate their cardinality
        for field in schema.fields() {
            let col_name = field.name();

            // Skip special columns
            if col_name == "time" || col_name == "measurement" || col_name == "date" {
                continue;
            }

            // Only analyze string columns (tags)
            if field.data_type() != &DataType::Utf8 {
                continue;
            }

            // Get the column and estimate cardinality
            if let Ok(col_idx) = schema.index_of(col_name) {
                let cardinality = Self::estimate_cardinality(batch.column(col_idx));
                tag_cardinalities.push((col_name.clone(), cardinality));
            }
        }

        // Sort by cardinality (ascending - low cardinality first)
        tag_cardinalities.sort_by_key(|(_, card)| *card);

        // Build final sort order: tags by cardinality, then time
        let mut sort_order: Vec<String> = tag_cardinalities
            .into_iter()
            .map(|(name, _)| name)
            .collect();

        // Always end with time
        sort_order.push("time".to_string());

        sort_order
    }

    /// Estimate cardinality of a column using sampling for large columns
    pub(crate) fn estimate_cardinality(column: &ArrayRef) -> usize {
        let string_array = match column.as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => return HIGH_CARDINALITY_THRESHOLD + 1, // Non-string, treat as high cardinality
        };

        let num_rows = string_array.len();
        if num_rows == 0 {
            return 0;
        }

        // For small columns, count exact cardinality
        // For large columns, sample to estimate (performance optimization)
        let sample_size = num_rows.min(CARDINALITY_SAMPLE_SIZE);
        let step = if num_rows > sample_size {
            num_rows / sample_size
        } else {
            1
        };

        let mut distinct_values: HashSet<Option<&str>> = HashSet::new();

        for i in (0..num_rows).step_by(step) {
            if string_array.is_null(i) {
                distinct_values.insert(None);
            } else {
                distinct_values.insert(Some(string_array.value(i)));
            }

            // Early exit if we've determined it's high cardinality
            if distinct_values.len() > HIGH_CARDINALITY_THRESHOLD {
                return usize::MAX;
            }
        }

        // If we sampled, extrapolate the cardinality
        if step > 1 {
            // Rough estimation: actual cardinality is likely higher than sampled
            let estimated = distinct_values.len() * 2;
            if estimated > HIGH_CARDINALITY_THRESHOLD {
                return usize::MAX;
            }
            return estimated;
        }

        distinct_values.len()
    }

    /// Sort a RecordBatch by multiple columns using lexicographic sorting
    pub(crate) fn sort_batch_by_columns(
        batch: &RecordBatch,
        sort_columns: &[String],
    ) -> Result<RecordBatch, String> {
        if sort_columns.is_empty() {
            return Ok(batch.clone());
        }

        let schema = batch.schema();

        // Build SortColumn specifications
        let mut sort_cols: Vec<SortColumn> = Vec::with_capacity(sort_columns.len());

        for col_name in sort_columns {
            if let Ok(idx) = schema.index_of(col_name) {
                sort_cols.push(SortColumn {
                    values: batch.column(idx).clone(),
                    options: None, // Default: ascending, nulls last
                });
            }
        }

        if sort_cols.is_empty() {
            return Ok(batch.clone());
        }

        // Perform lexicographic sort
        let indices = lexsort_to_indices(&sort_cols, None)
            .map_err(|e| format!("Failed to lexsort: {}", e))?;

        // Apply sort indices to all columns
        let sorted_columns: Result<Vec<ArrayRef>, String> = batch
            .columns()
            .iter()
            .map(|col| {
                take(col.as_ref(), &indices, None)
                    .map_err(|e| format!("Failed to reorder column: {}", e))
            })
            .collect();

        RecordBatch::try_new(schema, sorted_columns?)
            .map_err(|e| format!("Failed to create sorted batch: {}", e))
    }
}
