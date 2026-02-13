//! File discovery using ObjectStore API
//!
//! All file discovery operations use the ObjectStore abstraction,
//! which works uniformly with both local filesystem and remote
//! object storage (S3/MinIO/OSS).

use super::config::{ParquetFileInfo, ParquetTableConfig};
use super::filters::{extract_partition_filters, matches_partition_filter};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::prelude::Expr;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// Discover files for scan with partition filter pruning
///
/// This is the main entry point for file discovery. It uses ObjectStore
/// for all operations and applies partition filters to prune directories
/// before listing files, dramatically reducing list operations for large tables.
///
/// Path structure: base_path/date=YYYY-MM-DD/node_id=XXX/file.parquet
pub async fn discover_files(
    object_store: &Arc<dyn ObjectStore>,
    base_path: &str,
    config: &ParquetTableConfig,
    filters: &[Expr],
) -> Result<Vec<ParquetFileInfo>> {
    let base_prefix = ObjectPath::from(base_path.trim_start_matches('/'));

    // Extract partition filters for both date and node_id
    let date_filters = extract_partition_filters(&config.partition_column, filters);
    let node_id_filters = extract_partition_filters(&config.node_id_column, filters);

    // List date partition directories using list_with_delimiter
    let list_result = object_store
        .list_with_delimiter(Some(&base_prefix))
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut files = Vec::new();
    let date_prefix = format!("{}=", config.partition_column);
    let node_id_prefix = format!("{}=", config.node_id_column);

    // Process each date partition directory
    for date_dir in list_result.common_prefixes {
        let date_dir_name = date_dir
            .as_ref()
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or("");

        // Check if this is a date partition directory
        if !date_dir_name.starts_with(&date_prefix) {
            continue;
        }

        let date_value = date_dir_name
            .strip_prefix(&date_prefix)
            .unwrap_or("")
            .to_string();

        // Apply date partition filter pruning
        if !date_filters.is_empty() && !matches_partition_filter(&date_value, &date_filters) {
            debug!(
                "Pruning date partition {} (doesn't match filters)",
                date_value
            );
            continue;
        }

        // List node_id subdirectories within this date partition
        let node_id_list = object_store
            .list_with_delimiter(Some(&date_dir))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        for node_id_dir in node_id_list.common_prefixes {
            let node_id_dir_name = node_id_dir
                .as_ref()
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .unwrap_or("");

            // Check if this is a node_id partition directory
            if !node_id_dir_name.starts_with(&node_id_prefix) {
                continue;
            }

            let node_id_value = node_id_dir_name
                .strip_prefix(&node_id_prefix)
                .unwrap_or("")
                .to_string();

            // Apply node_id partition filter pruning
            if !node_id_filters.is_empty()
                && !matches_partition_filter(&node_id_value, &node_id_filters)
            {
                debug!(
                    "Pruning node_id partition {} (doesn't match filters)",
                    node_id_value
                );
                continue;
            }

            // List parquet files in this node_id directory
            let partition_files = object_store.list(Some(&node_id_dir));
            let objects: Vec<_> = partition_files
                .try_collect()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            for meta in objects {
                let path_str = meta.location.to_string();
                if !path_str.ends_with(".parquet") {
                    continue;
                }

                let mut partition_values = HashMap::new();
                partition_values.insert(config.partition_column.clone(), date_value.clone());
                partition_values.insert(config.node_id_column.clone(), node_id_value.clone());

                files.push(ParquetFileInfo {
                    path: path_str,
                    partition_values,
                    file_size: meta.size as u64,
                });
            }
        }
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}

/// Infer schema by merging schemas from ALL parquet files under the prefix
///
/// Different parquet files for the same table may have different column sets
/// (e.g., Telegraf sends separate points with different field sets for the same
/// measurement like `swap`: one batch has `free,total,used,used_percent` and
/// another has `in,out`). Reading only one file would miss fields from other
/// files, causing NULL-only rows in query results.
///
/// This function reads schemas from multiple files (up to MAX_FILES_FOR_SCHEMA)
/// and merges them into a unified schema containing all fields.
pub async fn infer_schema_from_prefix(
    object_store: &Arc<dyn ObjectStore>,
    base_path: &str,
    config: &ParquetTableConfig,
) -> Result<Schema> {
    use futures::StreamExt;
    use std::collections::HashSet;

    let prefix = ObjectPath::from(base_path.trim_start_matches('/'));

    // Read schemas from multiple files and merge them to capture all field sets.
    // Cap at MAX_FILES_FOR_SCHEMA to bound I/O cost; also stop early if the last
    // several files added no new fields (schema has converged).
    const MAX_FILES_FOR_SCHEMA: usize = 20;
    const CONVERGENCE_THRESHOLD: usize = 3;

    let mut list_stream = object_store.list(Some(&prefix));
    let mut merged_fields: Vec<Field> = Vec::new();
    let mut seen_names: HashSet<String> = HashSet::new();
    let mut files_read = 0;
    let mut files_without_new_fields = 0;

    while let Some(result) = list_stream.next().await {
        match result {
            Ok(meta) => {
                let path_str = meta.location.to_string();
                if !path_str.ends_with(".parquet") {
                    continue;
                }

                match infer_schema_from_path(object_store, &meta.location).await {
                    Ok(schema) => {
                        let mut found_new = false;
                        for field in schema.fields() {
                            if seen_names.insert(field.name().clone()) {
                                merged_fields.push(field.as_ref().clone());
                                found_new = true;
                            }
                        }
                        files_read += 1;

                        if found_new {
                            files_without_new_fields = 0;
                        } else {
                            files_without_new_fields += 1;
                        }

                        // Stop if we've read enough files or schema has converged
                        if files_read >= MAX_FILES_FOR_SCHEMA
                            || (files_read > 1
                                && files_without_new_fields >= CONVERGENCE_THRESHOLD)
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        debug!("Failed to read schema from {}: {}", path_str, e);
                    }
                }
            }
            Err(e) => {
                return Err(DataFusionError::External(Box::new(e)));
            }
        }
    }

    if merged_fields.is_empty() {
        // No parquet files found - return minimal schema with partition columns
        return Ok(Schema::new(vec![
            Field::new(&config.partition_column, DataType::Utf8, true),
            Field::new(&config.node_id_column, DataType::Utf8, true),
        ]));
    }

    debug!(
        "Inferred merged schema from {} parquet files: {} fields",
        files_read,
        merged_fields.len()
    );

    // Add partition columns if not already present
    if !seen_names.contains(&config.partition_column) {
        merged_fields.push(Field::new(&config.partition_column, DataType::Utf8, true));
    }
    if !seen_names.contains(&config.node_id_column) {
        merged_fields.push(Field::new(&config.node_id_column, DataType::Utf8, true));
    }

    Ok(Schema::new(merged_fields))
}

/// Infer schema from a specific parquet file path
pub async fn infer_schema_from_path(
    object_store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
) -> Result<Schema> {
    // Fetch the file content
    let get_result = object_store
        .get(path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let bytes = get_result
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Build reader and get schema
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Unwrap Dictionary-encoded types to their value types.
    // Parquet files may use dictionary encoding for string columns, but DataFusion's
    // aggregation operators (GROUP BY, avg, etc.) expect plain Utf8/LargeUtf8.
    let raw_schema = reader.schema();
    let fields: Vec<Field> = raw_schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Dictionary(_, value_type) => {
                Field::new(f.name(), value_type.as_ref().clone(), f.is_nullable())
            }
            _ => f.as_ref().clone(),
        })
        .collect();
    Ok(Schema::new(fields))
}

/// Discover files without filters (for initial schema inference)
///
/// This is a lighter version that just finds files without applying filters.
/// Used when we need to discover files but don't have filter expressions yet.
pub async fn discover_all_files(
    object_store: &Arc<dyn ObjectStore>,
    base_path: &str,
    config: &ParquetTableConfig,
) -> Result<Vec<ParquetFileInfo>> {
    discover_files(object_store, base_path, config, &[]).await
}
