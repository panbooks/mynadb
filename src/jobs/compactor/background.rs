//! Background Compaction Jobs
//!
//! Handles background LSM and full compaction tasks.
//! Uses Object Store abstraction for both local and S3/MinIO storage.

use crate::storage::layout::STORE_DIR;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::{CompactionConfig, CompactionResult, Compactor};

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
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

            info!(
                "Background LSM compaction started: every {} seconds",
                interval_secs
            );

            loop {
                ticker.tick().await;
                self.run_lsm_compaction_cycle(&config).await;
            }
        });
    }

    /// Run a single LSM compaction cycle across all catalogs
    async fn run_lsm_compaction_cycle(&self, config: &CompactionConfig) {
        let catalogs = match self.storage.list_prefixes(STORE_DIR).await {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to list catalogs for compaction: {}", e);
                return;
            }
        };

        for catalog_name in catalogs {
            if catalog_name.starts_with('.') {
                continue;
            }

            match self.compact_catalog(&catalog_name, config).await {
                Ok(results) => log_compaction_results("LSM", &results),
                Err(e) => error!("LSM compaction error for {}: {}", catalog_name, e),
            }
        }
    }

    /// Start background full compaction task
    /// This runs daily at a specified hour to fully compact yesterday's partitions
    pub fn start_background_full_compaction(
        self: Arc<Self>,
        check_interval_secs: u64,
        config: CompactionConfig,
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
                    Ok(results) => log_compaction_results("Full", &results),
                    Err(e) => error!("Full compaction error: {}", e),
                }
            }
        });
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
