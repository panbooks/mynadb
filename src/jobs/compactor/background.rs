//! Background Compaction Jobs
//!
//! Handles background LSM and full compaction tasks.
//! Uses Object Store abstraction for both local and S3/MinIO storage.

use crate::storage::layout::STORE_DIR;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::{CompactionConfig, CompactionResult, Compactor};

/// Callback type for invalidating caches after compaction.
/// Called with (catalog, schema, table) for each compacted table.
pub type CacheInvalidationFn = Arc<dyn Fn(&str, &str, &str) + Send + Sync>;

impl Compactor {
    /// Start background LSM compaction task for all catalogs
    ///
    /// This runs frequently (e.g., every 60s) to keep L0 files under control.
    /// Uses Object Store to discover catalogs (works with both local and S3).
    pub fn start_background_lsm_compaction(
        self: Arc<Self>,
        interval_secs: u64,
        config: CompactionConfig,
    ) {
        self.start_background_lsm_compaction_with_callback(interval_secs, config, None);
    }

    /// Start background LSM compaction with optional cache invalidation callback
    pub fn start_background_lsm_compaction_with_callback(
        self: Arc<Self>,
        interval_secs: u64,
        config: CompactionConfig,
        on_compacted: Option<CacheInvalidationFn>,
    ) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

            info!(
                "Background LSM compaction started: every {} seconds",
                interval_secs
            );

            loop {
                ticker.tick().await;
                let results = self.run_lsm_compaction_cycle(&config).await;
                if let Some(ref cb) = on_compacted {
                    invalidate_compacted_tables(&results, cb);
                }
            }
        });
    }

    /// Run a single LSM compaction cycle across all catalogs
    async fn run_lsm_compaction_cycle(&self, config: &CompactionConfig) -> Vec<CompactionResult> {
        let catalogs = match self.storage.list_prefixes(STORE_DIR).await {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to list catalogs for compaction: {}", e);
                return Vec::new();
            }
        };

        let mut all_results = Vec::new();
        for catalog_name in catalogs {
            if catalog_name.starts_with('.') {
                continue;
            }

            match self.compact_catalog(&catalog_name, config).await {
                Ok(results) => {
                    log_compaction_results("LSM", &results);
                    all_results.extend(results);
                }
                Err(e) => error!("LSM compaction error for {}: {}", catalog_name, e),
            }
        }
        all_results
    }

    /// Start background full compaction task
    /// This runs daily at a specified hour to fully compact yesterday's partitions
    pub fn start_background_full_compaction(
        self: Arc<Self>,
        check_interval_secs: u64,
        config: CompactionConfig,
    ) {
        self.start_background_full_compaction_with_callback(check_interval_secs, config, None);
    }

    /// Start background full compaction with optional cache invalidation callback
    pub fn start_background_full_compaction_with_callback(
        self: Arc<Self>,
        check_interval_secs: u64,
        config: CompactionConfig,
        on_compacted: Option<CacheInvalidationFn>,
    ) {
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(tokio::time::Duration::from_secs(check_interval_secs));

            info!(
                "Background full compaction started: checking every {} seconds (runs at hour {})",
                check_interval_secs, config.full_compaction_hour
            );

            loop {
                ticker.tick().await;

                match self.full_compact_yesterday(&config).await {
                    Ok(results) => {
                        log_compaction_results("Full", &results);
                        if let Some(ref cb) = on_compacted {
                            invalidate_compacted_tables(&results, cb);
                        }
                    }
                    Err(e) => error!("Full compaction error: {}", e),
                }
            }
        });
    }
}

/// Invalidate caches for each unique table that was compacted
fn invalidate_compacted_tables(results: &[CompactionResult], callback: &CacheInvalidationFn) {
    if results.is_empty() {
        return;
    }

    let mut seen: HashSet<(&str, &str, &str)> = HashSet::new();
    for r in results {
        if seen.insert((&r.catalog, &r.schema, &r.table)) {
            debug!(
                "Invalidating caches after compaction: {}.{}.{}",
                r.catalog, r.schema, r.table
            );
            callback(&r.catalog, &r.schema, &r.table);
        }
    }
}

/// Log compaction results if any partitions were compacted
fn log_compaction_results(compaction_type: &str, results: &[CompactionResult]) {
    if results.is_empty() {
        return;
    }

    info!("{} compaction: {} partitions compacted", compaction_type, results.len());

    for result in results {
        info!(
            "{}.{}.{}/{}: {} → {} files, {:.2} MB",
            result.catalog,
            result.schema,
            result.table,
            result.date_partition,
            result.files_before,
            result.files_after,
            result.bytes_before as f64 / 1024.0 / 1024.0
        );
    }
}
