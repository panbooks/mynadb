//! Parquet file reading logic using ObjectStore

use super::config::ParquetFileInfo;
use super::filters::{create_row_filter_from_predicates, extract_equality_predicates};
use datafusion::arrow::array::{new_null_array, Array, RecordBatch, RecordBatchOptions, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::prelude::Expr;
use futures::stream::{self, Stream};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tracing::debug;

/// Read a single Parquet file and return batches
///
/// Parameters:
/// - `file_info`: Information about the Parquet file to read
/// - `target_schema`: The projected schema (already subset based on projection)
/// - `_projection`: Original projection indices (unused - we use target_schema instead)
/// - `batch_size`: Batch size for reading
/// - `limit`: Optional row limit
/// - `partition_column`: Name of the date partition column (default: "date")
/// - `node_id_column`: Name of the node_id partition column (default: "node_id")
/// - `filters`: Pushed-down filter expressions for bloom filter and predicate pushdown
/// - `object_store`: ObjectStore for file access
/// - `parquet_cache`: Optional local cache for remote files (cluster mode)
#[allow(clippy::too_many_arguments)]
pub async fn read_parquet_file(
    file_info: &ParquetFileInfo,
    target_schema: SchemaRef,
    _projection: Option<Vec<usize>>,
    batch_size: usize,
    limit: Option<usize>,
    partition_column: &str,
    node_id_column: &str,
    filters: &[Expr],
    object_store: &Arc<dyn ObjectStore>,
    parquet_cache: Option<&Arc<crate::data::cache::ParquetLocalCache>>,
) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
    // Try to get from local cache first (for cluster mode)
    let file_bytes = if let Some(cache) = parquet_cache {
        match cache.get_or_fetch(&file_info.path).await {
            Ok(local_path) => {
                // Read from local cached file
                let file = std::fs::File::open(&local_path)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let mut reader = std::io::BufReader::new(file);
                let mut bytes = Vec::new();
                std::io::Read::read_to_end(&mut reader, &mut bytes)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                bytes::Bytes::from(bytes)
            }
            Err(e) => {
                debug!(
                    "Parquet cache fetch failed for {}: {}, fetching from object store",
                    file_info.path, e
                );
                // Fall back to object store
                fetch_from_object_store(object_store, &file_info.path).await?
            }
        }
    } else {
        // No cache, fetch directly from object store
        fetch_from_object_store(object_store, &file_info.path).await?
    };

    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file_bytes)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .with_batch_size(batch_size);

    // Build projection based on target schema field names
    // Map field names to file column indices
    let file_schema = builder.schema().clone();
    let file_projection: Vec<usize> = target_schema
        .fields()
        .iter()
        .filter(|field| {
            // Exclude partition columns - they will be added from partition path
            field.name() != partition_column && field.name() != node_id_column
        })
        .filter_map(|field| {
            file_schema
                .fields()
                .iter()
                .position(|f| f.name() == field.name())
        })
        .collect();

    if !file_projection.is_empty() {
        let projection_mask = datafusion::parquet::arrow::ProjectionMask::roots(
            builder.parquet_schema(),
            file_projection.clone(),
        );
        builder = builder.with_projection(projection_mask);
    }

    // Apply limit if specified
    if let Some(lim) = limit {
        builder = builder.with_limit(lim);
    }

    // Extract equality predicates for bloom filter optimization
    // These predicates on string columns will benefit from bloom filter checks
    let equality_predicates = extract_equality_predicates(filters, &file_schema);

    // Apply row filter for predicate pushdown
    // The parquet reader will use bloom filters (if available) during row group selection
    // and the row filter provides additional row-level filtering
    if !equality_predicates.is_empty() {
        let parquet_schema = builder.parquet_schema();
        if let Some(row_filter) =
            create_row_filter_from_predicates(equality_predicates.clone(), parquet_schema)
        {
            debug!(
                "Applying {} equality predicates to Parquet reader for bloom filter/predicate pushdown: {}",
                equality_predicates.len(),
                file_info.path
            );
            builder = builder.with_row_filter(row_filter);
        }
    }

    let reader = builder
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Clone partition values for the stream
    let partition_values = file_info.partition_values.clone();
    let partition_col = partition_column.to_string();
    let node_id_col = node_id_column.to_string();

    // Convert to stream and add partition columns if needed
    let batches: Vec<RecordBatch> = reader.filter_map(|r| r.ok()).collect();

    let result_batches: Vec<Result<RecordBatch>> = batches
        .into_iter()
        .map(|batch| {
            add_partition_columns(
                &batch,
                &target_schema,
                &partition_col,
                &node_id_col,
                &partition_values,
            )
        })
        .collect();

    Ok(Box::pin(stream::iter(result_batches)))
}

/// Fetch file bytes from ObjectStore
async fn fetch_from_object_store(
    object_store: &Arc<dyn ObjectStore>,
    path: &str,
) -> Result<bytes::Bytes> {
    let object_path = ObjectPath::from(path.trim_start_matches('/'));
    let get_result = object_store
        .get(&object_path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    get_result
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Add partition columns (date, node_id) to a record batch
fn add_partition_columns(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
    partition_column: &str,
    node_id_column: &str,
    partition_values: &HashMap<String, String>,
) -> Result<RecordBatch> {
    // Handle empty projection (e.g., count(*)) — return 0-column batch with correct row count
    if target_schema.fields().is_empty() {
        let options = datafusion::arrow::array::RecordBatchOptions::new()
            .with_row_count(Some(batch.num_rows()));
        return RecordBatch::try_new_with_options(target_schema.clone(), vec![], &options)
            .map_err(|e| DataFusionError::External(Box::new(e)));
    }

    let num_rows = batch.num_rows();

    // Check if any partition columns are in target schema
    let has_date_col = target_schema
        .fields()
        .iter()
        .any(|f| f.name() == partition_column);
    let has_node_id_col = target_schema
        .fields()
        .iter()
        .any(|f| f.name() == node_id_column);

    if !has_date_col && !has_node_id_col {
        // No partition columns requested in projection
        // Reproject batch to match target schema, casting types and null-filling as needed
        let columns: Vec<Arc<dyn Array>> = target_schema
            .fields()
            .iter()
            .map(|field| {
                match batch.schema().index_of(field.name()) {
                    Ok(idx) => {
                        let col = batch.column(idx);
                        if col.data_type() != field.data_type() {
                            cast(col.as_ref(), field.data_type()).unwrap_or_else(|_| col.clone())
                        } else {
                            col.clone()
                        }
                    }
                    Err(_) => new_null_array(field.data_type(), num_rows),
                }
            })
            .collect();

        return RecordBatch::try_new(target_schema.clone(), columns)
            .map_err(|e| DataFusionError::External(Box::new(e)));
    }

    // Build new columns array with partition columns in correct positions
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();

    for field in target_schema.fields() {
        if field.name() == partition_column {
            // Add date partition column with the partition value
            let value = partition_values
                .get(partition_column)
                .map(|s| s.as_str())
                .unwrap_or("");
            let array: Arc<dyn Array> = Arc::new(StringArray::from(vec![value; num_rows]));
            columns.push(array);
        } else if field.name() == node_id_column {
            // Add node_id partition column with the partition value
            let value = partition_values
                .get(node_id_column)
                .map(|s| s.as_str())
                .unwrap_or("");
            let array: Arc<dyn Array> = Arc::new(StringArray::from(vec![value; num_rows]));
            columns.push(array);
        } else {
            // Find and add column from batch
            match batch.schema().index_of(field.name()) {
                Ok(col_idx) => {
                    let col = batch.column(col_idx);
                    // Cast if types don't match (e.g., Dictionary(Int32, Utf8) → Utf8)
                    if col.data_type() != field.data_type() {
                        let casted = cast(col.as_ref(), field.data_type())
                            .map_err(|e| DataFusionError::ArrowError(e, None))?;
                        columns.push(casted);
                    } else {
                        columns.push(col.clone());
                    }
                }
                Err(_) => {
                    // Field not in this Parquet file — fill with nulls.
                    // Different files for the same table can have different columns
                    // (e.g., Telegraf's procstat has varying fields across intervals).
                    columns.push(new_null_array(field.data_type(), num_rows));
                }
            }
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}
