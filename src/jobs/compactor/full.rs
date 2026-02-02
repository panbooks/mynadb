//! Full Compaction Operations for Compactor
//!
//! Handles full compaction for yesterday's partitions (date-based).

use chrono::Timelike;
use futures::stream::{FuturesUnordered, StreamExt};
use std::fs;
use std::path::Path;
use std::time::Instant;
use tracing::{debug, info, warn};

use super::{
    CompactionConfig, CompactionResult, Compactor, DistributedLockGuard, MB_TO_BYTES,
    COMPACTION_LOCK_TTL_MS,
};

impl Compactor {
    /// Full compaction for yesterday's partitions (date-based, no metadata overhead)
    /// Now runs partitions in parallel (up to max_concurrent_compactions)
    pub async fn full_compact_yesterday(
        &self,
        config: &CompactionConfig,
    ) -> Result<Vec<CompactionResult>, String> {
        if !config.full_compaction_enabled {
            return Ok(Vec::new());
        }

        let now = chrono::Local::now();
        let current_hour = now.hour();

        // Only run at configured hour (default: 01:00)
        if current_hour != config.full_compaction_hour {
            return Ok(Vec::new());
        }

        // Calculate yesterday's date
        let yesterday = (now - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();
        let yesterday_partition = format!("date={}", yesterday);

        info!(
            "Starting full compaction for yesterday: {} (hour {})",
            yesterday_partition, current_hour
        );

        // Phase 1: Collect all partitions that need compaction
        let mut partitions_to_compact: Vec<(String, String, String, std::path::PathBuf)> =
            Vec::new();

        // Scan all catalogs
        let catalog_entries =
            fs::read_dir(&self.data_dir).map_err(|e| format!("Failed to read data dir: {}", e))?;

        for catalog_entry in catalog_entries.flatten() {
            if !catalog_entry.path().is_dir() {
                continue;
            }

            let catalog = match catalog_entry.file_name().to_str() {
                Some(name) if !name.starts_with('.') => name.to_string(),
                _ => continue,
            };

            // Scan schemas
            let schema_entries = match fs::read_dir(&catalog_entry.path()) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for schema_entry in schema_entries.flatten() {
                if !schema_entry.path().is_dir() {
                    continue;
                }

                let schema = match schema_entry.file_name().to_str() {
                    Some(name) if !name.starts_with('.') => name.to_string(),
                    _ => continue,
                };

                // Scan tables
                let table_entries = match fs::read_dir(&schema_entry.path()) {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                for table_entry in table_entries.flatten() {
                    if !table_entry.path().is_dir() {
                        continue;
                    }

                    let table = match table_entry.file_name().to_str() {
                        Some(name) => name.to_string(),
                        None => continue,
                    };

                    // Check if yesterday's partition exists
                    let partition_path = table_entry.path().join(&yesterday_partition);

                    if !partition_path.exists() {
                        continue;
                    }

                    // Add to compaction queue
                    partitions_to_compact.push((
                        catalog.clone(),
                        schema.clone(),
                        table.clone(),
                        partition_path,
                    ));
                }
            }
        }

        if partitions_to_compact.is_empty() {
            debug!(
                "No partitions found for full compaction on {}",
                yesterday_partition
            );
            return Ok(Vec::new());
        }

        info!(
            "Found {} partitions for full compaction (max {} concurrent)",
            partitions_to_compact.len(),
            config.full_compaction_max_concurrent
        );

        // Phase 2: Process partitions concurrently using FuturesUnordered
        let mut tasks = FuturesUnordered::new();

        for (catalog, schema, table, partition_path) in partitions_to_compact {
            // Clone strings to move into the future
            let catalog_owned = catalog.clone();
            let schema_owned = schema.clone();
            let table_owned = table.clone();
            let partition_name_owned = yesterday_partition.clone();
            let partition_path_owned = partition_path.clone();
            let config_clone = config.clone();

            let future = async move {
                self.full_compact_partition(
                    &catalog_owned,
                    &schema_owned,
                    &table_owned,
                    &partition_name_owned,
                    &partition_path_owned,
                    &config_clone,
                )
                .await
            };
            tasks.push(future);
        }

        // Collect results as they complete
        let mut results = Vec::new();
        let mut completed = 0;
        let total = tasks.len();

        while let Some(result) = tasks.next().await {
            completed += 1;
            match result {
                Ok(Some(r)) => {
                    info!(
                        "Full compaction [{}/{}]: {}.{}.{}/{} → {} files, {:.2} MB",
                        completed,
                        total,
                        r.catalog,
                        r.schema,
                        r.table,
                        r.date_partition,
                        r.files_after,
                        r.bytes_after as f64 / (1024.0 * 1024.0)
                    );
                    results.push(r);
                }
                Ok(None) => {
                    debug!(
                        "Full compaction [{}/{}]: skipped (no work needed)",
                        completed, total
                    );
                }
                Err(e) => {
                    warn!("Full compaction [{}/{}]: failed - {}", completed, total, e);
                }
            }
        }

        if !results.is_empty() {
            info!(
                "Full compaction complete for {}: {} partitions compacted",
                yesterday_partition,
                results.len()
            );
        }

        Ok(results)
    }

    /// Full compact a single partition
    /// Filters out files >= full_compaction_target_size to reduce I/O
    pub(crate) async fn full_compact_partition(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_name: &str,
        partition_path: &Path,
        config: &CompactionConfig,
    ) -> Result<Option<CompactionResult>, String> {
        // Acquire full compaction semaphore permit if concurrent limiting is enabled
        let _permit = if let Some(ref semaphore) = self.full_compaction_semaphore {
            Some(
                semaphore
                    .acquire()
                    .await
                    .map_err(|e| format!("Failed to acquire full compaction semaphore: {}", e))?,
            )
        } else {
            None
        };

        // Try to acquire distributed lock for cluster mode
        // Uses same lock key format as LSM compaction to ensure mutual exclusion
        let lock_key = format!("/compaction/{}/{}/{}/{}", catalog, schema, table, partition_name);
        let _distributed_lock_guard: Option<DistributedLockGuard> =
            if let Some(ref mc) = self.meta_client {
                match mc.try_lock(&lock_key, COMPACTION_LOCK_TTL_MS).await {
                    Ok(Some(token)) => {
                        debug!(
                            "Acquired distributed lock for full compaction {}/{}/{}/{} (token={})",
                            catalog, schema, table, partition_name, token
                        );
                        Some(DistributedLockGuard {
                            meta_client: Some(mc.clone()),
                            lock_key: lock_key.clone(),
                            fencing_token: token,
                        })
                    }
                    Ok(None) => {
                        // Lock held by another node - skip this partition
                        debug!(
                        "Skipping full compaction for {}/{}/{}/{}: partition locked by another node",
                        catalog, schema, table, partition_name
                    );
                        return Ok(None);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to acquire distributed lock for full compaction {}/{}/{}/{}: {}. \
                         Proceeding with local-only lock.",
                            catalog, schema, table, partition_name, e
                        );
                        None
                    }
                }
            } else {
                None
            };

        let start = Instant::now();

        // List all parquet files with their sizes
        let all_files: Vec<(std::path::PathBuf, u64)> = fs::read_dir(partition_path)
            .map_err(|e| format!("Failed to read partition: {}", e))?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path().is_file()
                    && e.path().extension().and_then(|s| s.to_str()) == Some("parquet")
            })
            .filter_map(|e| {
                let path = e.path();
                fs::metadata(&path).ok().map(|m| (path, m.len()))
            })
            .collect();

        if all_files.len() < 2 {
            debug!(
                "Partition {}/{}/{}/{} has {} file(s), skipping",
                catalog,
                schema,
                table,
                partition_name,
                all_files.len()
            );
            return Ok(None);
        }

        let files_before = all_files.len();
        let bytes_before: u64 = all_files.iter().map(|(_, size)| size).sum();

        // Filter out files >= full_compaction_target_size to reduce I/O
        // Already-large files don't need to be merged
        let compactable_files: Vec<(std::path::PathBuf, u64)> = all_files
            .iter()
            .filter(|(_, size)| *size < config.full_compaction_target_size)
            .cloned()
            .collect();

        let oversized_files: Vec<(std::path::PathBuf, u64)> = all_files
            .iter()
            .filter(|(_, size)| *size >= config.full_compaction_target_size)
            .cloned()
            .collect();

        let filtered_count = oversized_files.len();
        if filtered_count > 0 {
            let oversized_bytes: u64 = oversized_files.iter().map(|(_, size)| size).sum();
            debug!(
                "Filtered out {} oversized files ({:.2} MB) in partition {}/{}/{}/{}",
                filtered_count,
                oversized_bytes as f64 / MB_TO_BYTES as f64,
                catalog,
                schema,
                table,
                partition_name
            );
        }

        // If no compactable files or only 1 compactable file, skip
        if compactable_files.len() < 2 {
            debug!(
                "Partition {}/{}/{}/{}: {} compactable file(s) (< 2), skipping full compaction",
                catalog,
                schema,
                table,
                partition_name,
                compactable_files.len()
            );
            return Ok(None);
        }

        let compactable_bytes: u64 = compactable_files.iter().map(|(_, size)| size).sum();

        info!(
            "Full compaction: {}/{}/{}/{} ({} files, {:.2} MB total, compacting {} files/{:.2} MB, {} oversized files skipped)",
            catalog,
            schema,
            table,
            partition_name,
            files_before,
            bytes_before as f64 / MB_TO_BYTES as f64,
            compactable_files.len(),
            compactable_bytes as f64 / MB_TO_BYTES as f64,
            filtered_count
        );

        // Extract just the paths for reading
        let files_to_compact: Vec<std::path::PathBuf> =
            compactable_files.iter().map(|(p, _)| p.clone()).collect();

        // Read compactable files
        let batches = self.read_parquet_files(&files_to_compact).await?;
        let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

        // Write consolidated file
        let timestamp = chrono::Utc::now().timestamp_millis();
        let output = partition_path.join(format!("FULL_{}.parquet", timestamp));

        self.write_compacted_file(output.to_str().unwrap(), batches)
            .await?;

        let output_size = fs::metadata(&output)
            .map(|m| m.len())
            .unwrap_or(compactable_bytes);

        info!(
            "Created: {} ({} rows, {:.2} MB from {:.2} MB)",
            output.display(),
            total_rows,
            output_size as f64 / MB_TO_BYTES as f64,
            compactable_bytes as f64 / MB_TO_BYTES as f64
        );

        // Register new FULL file with metadata manager
        if let Some(ref manager) = self.metadata_manager {
            let file_info = Self::create_file_info(
                output.to_str().unwrap_or_default(),
                catalog,
                schema,
                table,
                partition_name,
                255, // FULL files use level 255 to distinguish from LSM levels
                total_rows as u64,
                output_size,
            );
            if let Err(e) = manager.register_file(file_info).await {
                warn!(
                    "Failed to register compacted file {}: {}",
                    output.display(),
                    e
                );
            }
        }

        // Delete old compacted files (keep oversized files)
        let mut deleted_paths: Vec<String> = Vec::new();
        for (file, _) in &compactable_files {
            if let Err(e) = fs::remove_file(file) {
                warn!("Failed to delete {}: {}", file.display(), e);
            } else {
                debug!("Deleted: {}", file.display());
                deleted_paths.push(file.to_string_lossy().to_string());
            }
        }

        // Remove deleted files from metadata manager
        if let Some(ref manager) = self.metadata_manager {
            if !deleted_paths.is_empty() {
                if let Err(e) = manager.remove_files(deleted_paths).await {
                    warn!("Failed to remove files from metadata: {}", e);
                }
                // Save metadata after full compaction
                if let Err(e) = manager.save().await {
                    warn!("Failed to save metadata after full compaction: {}", e);
                }
            }
        }

        let files_after = 1 + filtered_count; // 1 new FULL file + oversized files remaining

        info!(
            "Full compaction complete: {}/{}/{}/{}: {} → {} files ({} compacted, {} oversized kept), {:.2} MB → {:.2} MB in {:?}",
            catalog,
            schema,
            table,
            partition_name,
            files_before,
            files_after,
            compactable_files.len(),
            filtered_count,
            bytes_before as f64 / MB_TO_BYTES as f64,
            (output_size + oversized_files.iter().map(|(_, s)| s).sum::<u64>()) as f64 / MB_TO_BYTES as f64,
            start.elapsed()
        );

        Ok(Some(CompactionResult {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            date_partition: partition_name.to_string(),
            l0_files_before: files_before,
            l0_files_after: 0,
            files_before,
            files_after,
            bytes_before,
            bytes_after: output_size + oversized_files.iter().map(|(_, s)| s).sum::<u64>(),
            rows_compacted: total_rows,
        }))
    }
}
