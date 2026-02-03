//! Parquet I/O Operations for Compactor
//!
//! Handles reading and writing parquet files.

use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::fs::{self, File};
use std::path::PathBuf;
use tracing::{debug, warn};

use crate::query::catalog::FileInfo;

use super::{CompactionConfig, Compactor, ParquetFileInfo};

impl Compactor {
    /// Create a FileInfo struct for registering with the metadata manager
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

    /// List all parquet files with level information
    pub(crate) fn list_parquet_file_infos(
        &self,
        dir: &str,
    ) -> Result<Vec<ParquetFileInfo>, String> {
        let entries = fs::read_dir(dir).map_err(|e| format!("Failed to read directory: {}", e))?;

        let mut file_infos = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "parquet" {
                        if let Ok(info) = ParquetFileInfo::from_path(path) {
                            file_infos.push(info);
                        }
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

    /// Read all batches from multiple parquet files
    pub(crate) async fn read_parquet_files(
        &self,
        files: &[PathBuf],
    ) -> Result<Vec<RecordBatch>, String> {
        let mut all_batches = Vec::new();
        let mut total_bytes_read = 0u64;

        for file_path in files {
            let file = File::open(file_path)
                .map_err(|e| format!("Failed to open file {}: {}", file_path.display(), e))?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| format!("Failed to create parquet reader: {}", e))?;

            let reader = builder
                .build()
                .map_err(|e| format!("Failed to build reader: {}", e))?;

            for batch_result in reader {
                let batch = batch_result.map_err(|e| format!("Failed to read batch: {}", e))?;

                // Calculate batch size (approximate)
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
            "Read {} batches ({} MB)",
            all_batches.len(),
            total_bytes_read / (1024 * 1024)
        );

        // Merge schemas and align batches if there are schema conflicts
        if all_batches.len() > 1 {
            all_batches = self.align_batch_schemas(all_batches)?;
        }

        Ok(all_batches)
    }

    /// Write compacted batches to a new file (merge-sorted by time, optimized)
    /// Uses k-way merge since each input file is already sorted
    pub(crate) async fn write_compacted_file(
        &self,
        filename: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), String> {
        if batches.is_empty() {
            return Err("No batches to write".to_string());
        }

        let schema = batches[0].schema();

        // K-way merge sort (efficient since each batch is already sorted)
        let sorted_batch = Self::merge_sorted_batches(batches, &schema)?;

        let total_rows = sorted_batch.num_rows();
        debug!(
            "Writing {} rows to {} (merge-sorted by time)",
            total_rows, filename
        );

        let file = File::create(filename).map_err(|e| format!("Failed to create file: {}", e))?;

        // Use optimized writer properties
        let props = Self::create_optimized_writer_props(&schema);

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
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

        debug!(
            "Wrote {} rows, {} MB (merge-sorted, compressed)",
            total_rows,
            batch_bytes / (1024 * 1024)
        );

        Ok(())
    }
}
