//! Parquet I/O Operations for Compactor
//!
//! Handles reading and writing parquet files.
//! Uses Object Store abstraction for both local and S3/MinIO storage.

use arrow::array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::io::Cursor;
use tracing::{debug, warn};

use crate::query::catalog::FileInfo;

use super::{CompactionConfig, Compactor, ParquetFileInfo};

impl Compactor {
    /// Create a FileInfo struct for registering with the metadata manager
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_file_info(
        path: &str,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
        level: u8,
        row_count: u64,
        size_bytes: u64,
    ) -> FileInfo {
        let file_name = path.rsplit('/').next().unwrap_or(path).to_string();

        FileInfo {
            path: path.to_string(),
            file_name,
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            partition_key: partition_key.to_string(),
            size_bytes,
            row_count,
            level,
            min_timestamp: None,
            max_timestamp: None,
            created_at: chrono::Utc::now().timestamp_millis(),
            row_group_count: 1,
            column_stats: None,
            etag: None,
        }
    }

    /// List all parquet files with level information using Object Store
    ///
    /// Recursively lists files in node_id= subdirectories.
    /// Path structure: catalog/schema/table/date=YYYY-MM-DD/node_id=X/*.parquet
    pub(crate) async fn list_parquet_file_infos_async(
        &self,
        prefix: &str,
    ) -> Result<Vec<ParquetFileInfo>, String> {
        // List all files recursively (includes node_id= subdirectories)
        let files = self.storage.list(prefix).await?;

        let mut file_infos = Vec::new();
        for path in files {
            if path.ends_with(".parquet") {
                // Get file size
                if let Ok(size) = self.storage.size(&path).await {
                    if let Ok(info) = ParquetFileInfo::from_path_and_size(path, size) {
                        file_infos.push(info);
                    }
                }
            }
        }

        Ok(file_infos)
    }

    /// Group files by size to avoid exceeding target size
    pub(crate) fn group_files_by_size(
        &self,
        files: &[ParquetFileInfo],
        config: &CompactionConfig,
    ) -> Result<Vec<Vec<ParquetFileInfo>>, String> {
        let mut groups = Vec::new();
        let mut current_group = Vec::new();
        let mut current_size = 0u64;

        for file in files {
            // Start new group if adding this file would exceed target size
            if !current_group.is_empty() && current_size + file.size > config.target_file_size {
                groups.push(current_group.clone());
                current_group.clear();
                current_size = 0;
            }

            current_group.push(file.clone());
            current_size += file.size;
        }

        // Add remaining files
        if !current_group.is_empty() {
            groups.push(current_group);
        }

        Ok(groups)
    }

    /// Read all batches from multiple parquet files using Object Store
    ///
    /// Works with both local and S3/MinIO storage via the Storage abstraction.
    pub(crate) async fn read_parquet_files_from_storage(
        &self,
        file_paths: &[String],
    ) -> Result<Vec<RecordBatch>, String> {
        let mut all_batches = Vec::new();
        let mut total_bytes_read = 0u64;

        for file_path in file_paths {
            // Read file bytes from Object Store
            let bytes = self
                .storage
                .get(file_path)
                .await
                .map_err(|e| format!("Failed to read file {}: {}", file_path, e))?;

            // Create a reader from bytes
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .map_err(|e| format!("Failed to create parquet reader for {}: {}", file_path, e))?;

            let reader = builder
                .build()
                .map_err(|e| format!("Failed to build reader for {}: {}", file_path, e))?;

            for batch_result in reader {
                let batch = batch_result
                    .map_err(|e| format!("Failed to read batch from {}: {}", file_path, e))?;

                let batch_bytes = batch.get_array_memory_size() as u64;
                total_bytes_read += batch_bytes;

                // Apply read rate limiting
                if let Some(ref limiter) = self.read_rate_limiter {
                    limiter.wait_for_bytes(batch_bytes as usize).await;
                }

                all_batches.push(batch);
            }
        }

        debug!(
            "Read {} batches ({} MB) from Object Store",
            all_batches.len(),
            total_bytes_read / (1024 * 1024)
        );

        // Merge schemas and align batches if there are schema conflicts
        if all_batches.len() > 1 {
            all_batches = self.align_batch_schemas(all_batches)?;
        }

        Ok(all_batches)
    }

    /// Write compacted batches to Object Store (merge-sorted by time, optimized)
    ///
    /// Uses k-way merge since each input file is already sorted.
    /// Returns the size of the written file in bytes.
    pub(crate) async fn write_compacted_file_to_storage(
        &self,
        path: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<u64, String> {
        if batches.is_empty() {
            return Err("No batches to write".to_string());
        }

        let schema = batches[0].schema();

        // K-way merge sort (efficient since each batch is already sorted)
        let sorted_batch = Self::merge_sorted_batches(batches, &schema)?;

        let total_rows = sorted_batch.num_rows();
        debug!(
            "Writing {} rows to {} (merge-sorted by time)",
            total_rows, path
        );

        // Write to in-memory buffer first
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);

        // Use optimized writer properties
        let props = Self::create_optimized_writer_props(&schema);

        let mut writer = ArrowWriter::try_new(cursor, schema.clone(), Some(props))
            .map_err(|e| format!("Failed to create writer: {}", e))?;

        let batch_bytes = sorted_batch.get_array_memory_size() as u64;

        writer
            .write(&sorted_batch)
            .map_err(|e| format!("Failed to write batch: {}", e))?;

        // Apply write rate limiting
        if let Some(ref limiter) = self.write_rate_limiter {
            limiter.wait_for_bytes(batch_bytes as usize).await;
        }

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        let file_size = buffer.len() as u64;

        // Write to Object Store
        self.storage
            .put(path, Bytes::from(buffer))
            .await
            .map_err(|e| format!("Failed to write to Object Store: {}", e))?;

        debug!(
            "Wrote {} rows, {} MB to Object Store (merge-sorted, compressed)",
            total_rows,
            file_size / (1024 * 1024)
        );

        Ok(file_size)
    }
}
