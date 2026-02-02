//! Partition Compaction Operations for Compactor
//!
//! Handles partition-level and level-based compaction.

use std::fs;
use std::path::Path;
use std::time::Instant;
use tracing::{debug, info, warn};

use super::{
    CompactionConfig, CompactionResult, Compactor, DistributedLockGuard, ParquetFileInfo,
    COMPACTION_LOCK_TTL_MS, LEVEL_PREFIX, LEVEL_SEPARATOR, PARQUET_EXT,
};

impl Compactor {
    /// Compact a non-partitioned table (e.g., pmc)
    pub(crate) async fn compact_non_partitioned_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        config: &CompactionConfig,
    ) -> Result<Option<CompactionResult>, String> {
        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);
        self.compact_path_internal(
            catalog,
            schema,
            table,
            "",
            &table_path,
            "[non-partitioned]",
            config,
        )
        .await
    }

    /// Compact a specific partition within a table
    pub async fn compact_table_partition(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date_partition: &str,
        config: &CompactionConfig,
    ) -> Result<Option<CompactionResult>, String> {
        let partition_path = format!(
            "{}/{}/{}/{}/{}",
            self.data_dir, catalog, schema, table, date_partition
        );
        self.compact_path_internal(
            catalog,
            schema,
            table,
            date_partition,
            &partition_path,
            date_partition,
            config,
        )
        .await
    }

    /// Internal compaction logic shared by partitioned and non-partitioned tables
    pub(crate) async fn compact_path_internal(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_id: &str,
        path: &str,
        display_partition: &str,
        config: &CompactionConfig,
    ) -> Result<Option<CompactionResult>, String> {
        // Acquire semaphore permit if concurrent limiting is enabled
        let _permit = if let Some(ref semaphore) = self.compaction_semaphore {
            Some(
                semaphore
                    .acquire()
                    .await
                    .map_err(|e| format!("Failed to acquire semaphore: {}", e))?,
            )
        } else {
            None
        };

        // Try to acquire distributed lock for cluster mode
        // This prevents multiple nodes from compacting the same partition simultaneously
        // Lock key format: /compaction/{catalog}/{schema}/{table}/{partition}
        let lock_key = format!("/compaction/{}/{}/{}/{}", catalog, schema, table, partition_id);
        let _distributed_lock_guard: Option<DistributedLockGuard> =
            if let Some(ref mc) = self.meta_client {
                match mc.try_lock(&lock_key, COMPACTION_LOCK_TTL_MS).await {
                    Ok(Some(token)) => {
                        debug!(
                            "Acquired distributed compaction lock for {}/{}/{}/{} (token={})",
                            catalog, schema, table, partition_id, token
                        );
                        Some(DistributedLockGuard {
                            meta_client: Some(mc.clone()),
                            lock_key: lock_key.clone(),
                            fencing_token: token,
                        })
                    }
                    Ok(None) => {
                        // Lock held by another node - skip this partition
                        // Another node is already compacting it
                        debug!(
                            "Skipping compaction for {}/{}/{}/{}: partition locked by another node",
                            catalog, schema, table, partition_id
                        );
                        return Ok(None);
                    }
                    Err(e) => {
                        // Meta service unavailable - log warning and proceed with local-only compaction
                        // This is a fail-open approach: better to risk duplicate work than to block
                        // compaction entirely when meta is temporarily unavailable
                        warn!(
                            "Failed to acquire distributed lock for compaction {}/{}/{}/{}: {}. \
                         Proceeding with local-only lock (potential duplicate work in cluster).",
                            catalog, schema, table, partition_id, e
                        );
                        None
                    }
                }
            } else {
                // Standalone mode - no distributed locking needed
                None
            };

        let start = Instant::now();
        let lock = self.get_lock(catalog, schema, table, partition_id).await;
        let _guard = lock.lock().await;

        if !Path::new(path).exists() {
            return Err(format!(
                "Path '{}.{}.{}/{}' does not exist",
                catalog, schema, table, display_partition
            ));
        }

        // List and parse all parquet files
        let mut file_infos = self.list_parquet_file_infos(path)?;

        if file_infos.is_empty() {
            return Ok(None);
        }

        // Sort by level (ascending) then timestamp (ascending)
        file_infos.sort_by(|a, b| {
            a.level
                .cmp(&b.level)
                .then_with(|| a.timestamp.cmp(&b.timestamp))
        });

        let total_files_before = file_infos.len();
        let l0_files_before = file_infos.iter().filter(|f| f.level == 0).count();
        let total_bytes_before: u64 = file_infos.iter().map(|f| f.size).sum();

        debug!(
            "Compacting {}.{}.{}/{}: {} files ({} L0), {:.2} MB",
            catalog,
            schema,
            table,
            display_partition,
            total_files_before,
            l0_files_before,
            total_bytes_before as f64 / 1024.0 / 1024.0
        );

        // Perform multi-level compaction
        let mut total_rows = 0;
        let mut compaction_happened = false;

        // Step 1: Minor compaction (L0 → L1) - highest priority
        if l0_files_before >= config.min_files {
            let l0_files: Vec<_> = file_infos
                .iter()
                .filter(|f| f.level == 0)
                .cloned()
                .collect();

            info!("Minor compaction: {} L0 files → L1", l0_files.len());

            let rows = self
                .compact_level_files(
                    path,
                    &l0_files,
                    0,
                    config,
                    catalog,
                    schema,
                    table,
                    partition_id,
                )
                .await?;
            total_rows += rows;
            compaction_happened = true;
        }

        // Step 2: Major compactions (L1 → L2, L2 → L3, etc.)
        let file_infos_updated = self.list_parquet_file_infos(path)?;

        for current_level in 1..=config.max_level {
            let all_level_files: Vec<_> = file_infos_updated
                .iter()
                .filter(|f| f.level == current_level)
                .cloned()
                .collect();

            // Filter out files >= target_file_size to reduce I/O
            let level_files: Vec<_> = all_level_files
                .iter()
                .filter(|f| f.size < config.target_file_size)
                .cloned()
                .collect();

            let filtered_count = all_level_files.len() - level_files.len();
            if filtered_count > 0 {
                debug!(
                    "Filtered out {} oversized files at L{} (>= {:.2} MB)",
                    filtered_count,
                    current_level,
                    config.target_file_size as f64 / 1024.0 / 1024.0
                );
            }

            if level_files.len() >= config.major_min_files {
                if current_level < config.max_level {
                    info!(
                        "Major compaction: {} L{} files → L{} ({} oversized files skipped)",
                        level_files.len(),
                        current_level,
                        current_level + 1,
                        filtered_count
                    );
                } else {
                    info!(
                        "Max-level compaction: {} L{} files → L{} (same level, {} oversized files skipped)",
                        level_files.len(),
                        current_level,
                        current_level,
                        filtered_count
                    );
                }

                let rows = self
                    .compact_level_files(
                        path,
                        &level_files,
                        current_level,
                        config,
                        catalog,
                        schema,
                        table,
                        partition_id,
                    )
                    .await?;
                total_rows += rows;
                compaction_happened = true;
            }
        }

        // Save metadata after all compaction operations complete
        if compaction_happened {
            if let Some(ref manager) = self.metadata_manager {
                if let Err(e) = manager.save().await {
                    warn!("Failed to save metadata after LSM compaction: {}", e);
                }
            }
        }

        if !compaction_happened {
            return Ok(None);
        }

        // Recalculate files after compaction
        let file_infos_after = self.list_parquet_file_infos(path)?;
        let total_files_after = file_infos_after.len();
        let l0_files_after = file_infos_after.iter().filter(|f| f.level == 0).count();
        let total_bytes_after: u64 = file_infos_after.iter().map(|f| f.size).sum();

        info!(
            "Compaction complete: {}.{}.{}/{}: {} -> {} files, {:.2} MB -> {:.2} MB in {:?}",
            catalog,
            schema,
            table,
            display_partition,
            total_files_before,
            total_files_after,
            total_bytes_before as f64 / 1024.0 / 1024.0,
            total_bytes_after as f64 / 1024.0 / 1024.0,
            start.elapsed()
        );

        Ok(Some(CompactionResult {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            date_partition: display_partition.to_string(),
            l0_files_before,
            l0_files_after,
            files_before: total_files_before,
            files_after: total_files_after,
            bytes_before: total_bytes_before,
            bytes_after: total_bytes_after,
            rows_compacted: total_rows,
        }))
    }

    /// Compact files at a specific level to the next level
    /// If at max_level, compacts within the same level to reduce file count
    /// Returns the total number of rows compacted
    pub(crate) async fn compact_level_files(
        &self,
        partition_path: &str,
        level_files: &[ParquetFileInfo],
        current_level: u32,
        config: &CompactionConfig,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_key: &str,
    ) -> Result<usize, String> {
        if level_files.is_empty() {
            return Ok(0);
        }

        // At max_level, compact within same level; otherwise, compact to next level
        let target_level = if current_level >= config.max_level {
            current_level // Stay at max level
        } else {
            current_level + 1 // Promote to next level
        };

        let mut total_rows = 0;

        // Group files by size to avoid exceeding target file size
        let file_groups = self.group_files_by_size(level_files, config)?;

        for (group_idx, group) in file_groups.iter().enumerate() {
            // Need at least 2 files to compact (unless it's the only group)
            if group.len() < 2 && file_groups.len() > 1 {
                continue;
            }

            debug!(
                "Compacting L{} group {}: {} files",
                current_level,
                group_idx + 1,
                group.len()
            );

            let paths: Vec<std::path::PathBuf> = group.iter().map(|f| f.path.clone()).collect();
            let batches = self.read_parquet_files(&paths).await?;
            let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
            total_rows += rows;

            // Create compacted file at target level
            let timestamp = chrono::Utc::now().timestamp_millis();
            let compacted_filename = format!(
                "{}/{}{}{}{}{}",
                partition_path, LEVEL_PREFIX, target_level, LEVEL_SEPARATOR, timestamp, PARQUET_EXT
            );

            self.write_compacted_file(&compacted_filename, batches)
                .await?;

            // Get the size of the newly written file for metadata
            let file_size = fs::metadata(&compacted_filename)
                .map(|m| m.len())
                .unwrap_or(0);

            // Register new file with metadata manager
            if let Some(ref manager) = self.metadata_manager {
                let file_info = Self::create_file_info(
                    &compacted_filename,
                    catalog,
                    schema,
                    table,
                    partition_key,
                    target_level as u8,
                    rows as u64,
                    file_size,
                );
                if let Err(e) = manager.register_file(file_info).await {
                    warn!(
                        "Failed to register compacted file {}: {}",
                        compacted_filename, e
                    );
                }
            }

            if target_level == current_level {
                info!(
                    "Created L{} file: {} ({} rows from {} L{} files) [max-level compaction]",
                    target_level,
                    Path::new(&compacted_filename)
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap(),
                    rows,
                    group.len(),
                    current_level
                );
            } else {
                info!(
                    "Created L{} file: {} ({} rows from {} L{} files)",
                    target_level,
                    Path::new(&compacted_filename)
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap(),
                    rows,
                    group.len(),
                    current_level
                );
            }

            // Delete old files after successful write
            let mut deleted_paths: Vec<String> = Vec::new();
            for file in &paths {
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
                }
            }
        }

        Ok(total_rows)
    }
}
