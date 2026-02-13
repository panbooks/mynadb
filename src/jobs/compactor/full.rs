//! Full Compaction Operations for Compactor
//!
//! Handles full compaction for yesterday's partitions (date-based).
//! Uses Object Store abstraction for both local and S3/MinIO storage.

use crate::storage::layout::STORE_DIR;
use crate::storage::manifest;
use chrono::Timelike;
use futures::stream::{FuturesUnordered, StreamExt};
use std::time::Instant;
use tracing::{debug, info, warn};

use super::{
    CompactionConfig, CompactionResult, Compactor, DistributedLockGuard, COMPACTION_LOCK_TTL_MS,
    MB_TO_BYTES,
};

impl Compactor {
    /// Discover all partition prefixes containing parquet files for a given date partition.
    ///
    /// Walks catalog → schema → table → date, handling both new layout (node_id= subdirs)
    /// and legacy layout (files directly under date=).
    async fn discover_compaction_targets(
        &self,
        yesterday_partition: &str,
    ) -> Vec<(String, String, String, String)> {
        let catalogs = self
            .storage
            .list_prefixes(STORE_DIR)
            .await
            .unwrap_or_else(|e| {
                warn!("Failed to list catalogs: {}", e);
                Vec::new()
            });

        let mut targets = Vec::new();

        for catalog in catalogs {
            if catalog.starts_with('.') {
                continue;
            }

            let schemas = self
                .storage
                .list_prefixes(&format!("{}/{}", STORE_DIR, catalog))
                .await
                .unwrap_or_default();

            for schema in schemas {
                if schema.starts_with('.') || schema.ends_with(".json") {
                    continue;
                }

                let tables = self
                    .storage
                    .list_prefixes(&format!("{}/{}/{}", STORE_DIR, catalog, schema))
                    .await
                    .unwrap_or_default();

                for table in tables {
                    let date_prefix = format!(
                        "{}/{}/{}/{}/{}",
                        STORE_DIR, catalog, schema, table, yesterday_partition
                    );
                    self.collect_partition_entries(
                        &catalog,
                        &schema,
                        &table,
                        &date_prefix,
                        &mut targets,
                    )
                    .await;
                }
            }
        }

        targets
    }

    /// Collect compaction entries for a single date prefix, handling node_id= vs legacy layout.
    async fn collect_partition_entries(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        date_prefix: &str,
        targets: &mut Vec<(String, String, String, String)>,
    ) {
        let node_dirs = self
            .storage
            .list_prefixes(date_prefix)
            .await
            .unwrap_or_default();

        if node_dirs.iter().any(|d| d.starts_with("node_id=")) {
            for node_dir in &node_dirs {
                if !node_dir.starts_with("node_id=") {
                    continue;
                }
                let partition_prefix = format!("{}/{}", date_prefix, node_dir);
                if self.has_parquet_files(&partition_prefix).await {
                    targets.push((
                        catalog.to_string(),
                        schema.to_string(),
                        table.to_string(),
                        partition_prefix,
                    ));
                }
            }
        } else if self.has_parquet_files(date_prefix).await {
            targets.push((
                catalog.to_string(),
                schema.to_string(),
                table.to_string(),
                date_prefix.to_string(),
            ));
        }
    }

    /// Check if a prefix contains any parquet files.
    async fn has_parquet_files(&self, prefix: &str) -> bool {
        self.storage
            .list(prefix)
            .await
            .map(|files| files.iter().any(|f| f.ends_with(".parquet")))
            .unwrap_or(false)
    }

    /// Full compaction for yesterday's partitions (date-based, no metadata overhead)
    ///
    /// Runs partitions in parallel (up to max_concurrent_compactions).
    /// Uses Object Store to discover catalogs, schemas, tables, and partitions.
    ///
    /// Path structure: catalog/schema/table/date=YYYY-MM-DD/node_id=X/*.parquet
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
        let partitions_to_compact = self
            .discover_compaction_targets(&yesterday_partition)
            .await;

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

        for (catalog, schema, table, partition_prefix) in partitions_to_compact {
            let catalog_owned = catalog.clone();
            let schema_owned = schema.clone();
            let table_owned = table.clone();
            let partition_name_owned = yesterday_partition.clone();
            let partition_prefix_owned = partition_prefix.clone();
            let config_clone = config.clone();

            let future = async move {
                self.full_compact_partition_async(
                    &catalog_owned,
                    &schema_owned,
                    &table_owned,
                    &partition_name_owned,
                    &partition_prefix_owned,
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

    /// Full compact a single partition using Object Store
    ///
    /// Filters out files >= full_compaction_target_size to reduce I/O.
    /// Handles node_id= subdirectories in the partition.
    ///
    /// Path structure: catalog/schema/table/date=YYYY-MM-DD/node_id=X/*.parquet
    pub(crate) async fn full_compact_partition_async(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition_name: &str,
        partition_prefix: &str,
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
        let lock_key = format!(
            "/compaction/{}/{}/{}/{}",
            catalog, schema, table, partition_name
        );
        let _distributed_lock_guard: Option<DistributedLockGuard> = if let Some(ref mc) =
            self.meta_client
        {
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

        // List all parquet files with their sizes using Object Store
        // This recursively lists files in node_id= subdirectories
        let all_file_paths = self.storage.list(partition_prefix).await?;
        let mut all_files: Vec<(String, u64)> = Vec::new();

        for path in all_file_paths {
            if path.ends_with(".parquet") {
                if let Ok(size) = self.storage.size(&path).await {
                    all_files.push((path, size));
                }
            }
        }

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

        // Filter out files >= full_compaction_target_size
        let compactable_files: Vec<(String, u64)> = all_files
            .iter()
            .filter(|(_, size)| *size < config.full_compaction_target_size)
            .cloned()
            .collect();

        let oversized_files: Vec<(String, u64)> = all_files
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

        // Read compactable files using Object Store
        let file_paths: Vec<String> = compactable_files.iter().map(|(p, _)| p.clone()).collect();
        let batches = self.read_parquet_files_from_storage(&file_paths).await?;
        let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

        // Write consolidated file
        let timestamp = chrono::Utc::now().timestamp_millis();
        let output_path = format!("{}/FULL_{}.parquet", partition_prefix, timestamp);

        let output_size = self
            .write_compacted_file_to_storage(&output_path, batches)
            .await?;

        info!(
            "Created: {} ({} rows, {:.2} MB from {:.2} MB)",
            output_path,
            total_rows,
            output_size as f64 / MB_TO_BYTES as f64,
            compactable_bytes as f64 / MB_TO_BYTES as f64
        );

        // Register new FULL file with metadata manager
        if let Some(ref manager) = self.metadata_manager {
            let file_info = Self::create_file_info(
                &output_path,
                catalog,
                schema,
                table,
                partition_name,
                255, // FULL files use level 255
                total_rows as u64,
                output_size,
            );
            if let Err(e) = manager.register_file(file_info).await {
                warn!("Failed to register compacted file {}: {}", output_path, e);
            }
        }

        // Mark source files as superseded in manifest BEFORE deletion
        // so queries immediately stop reading them even if deletion fails
        let source_filenames: Vec<String> = compactable_files
            .iter()
            .map(|(p, _)| manifest::filename_from_path(p).to_string())
            .collect();
        if let Err(e) =
            manifest::mark_files_superseded(&self.storage, partition_prefix, &source_filenames)
                .await
        {
            warn!("Failed to update compaction manifest: {}", e);
        }

        // Delete old compacted files using Object Store
        let mut deleted_paths: Vec<String> = Vec::new();
        for (file_path, _) in &compactable_files {
            if let Err(e) = self.storage.delete(file_path).await {
                warn!("Failed to delete {}: {}", file_path, e);
            } else {
                debug!("Deleted: {}", file_path);
                deleted_paths.push(file_path.clone());
            }
        }

        // Remove deleted files from metadata manager
        if let Some(ref manager) = self.metadata_manager {
            if !deleted_paths.is_empty() {
                if let Err(e) = manager.remove_files(deleted_paths).await {
                    warn!("Failed to remove files from metadata: {}", e);
                }
                if let Err(e) = manager.save().await {
                    warn!("Failed to save metadata after full compaction: {}", e);
                }
            }
        }

        let files_after = 1 + filtered_count;

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
