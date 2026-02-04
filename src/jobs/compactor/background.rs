//! Background Compaction Jobs
//!
//! Handles background LSM and full compaction tasks.
//! Uses Object Store abstraction for both local and S3/MinIO storage.

use std::sync::Arc;
use tracing::{error, info, warn};

use super::{CompactionConfig, Compactor};

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

                // Discover catalogs using Object Store
                match self.storage.list_prefixes("").await {
                    Ok(catalogs) => {
                        for catalog_name in catalogs {
                            // Skip hidden directories
                            if catalog_name.starts_with('.') {
                                continue;
                            }

                            // LSM compaction for this catalog
                            match self.compact_catalog(&catalog_name, &config).await {
                                Ok(results) => {
                                    if !results.is_empty() {
                                        info!(
                                            "LSM compaction: {} partitions compacted",
                                            results.len()
                                        );
                                        for result in &results {
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
                                }
                                Err(e) => {
                                    error!("LSM compaction error for {}: {}", catalog_name, e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to list catalogs for compaction: {}", e);
                    }
                }
            }
        });
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

                // Full compaction for yesterday's partitions (checks hour internally)
                match self.full_compact_yesterday(&config).await {
                    Ok(results) => {
                        if !results.is_empty() {
                            info!("Full compaction: {} partitions compacted", results.len());
                            for result in &results {
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
                    }
                    Err(e) => {
                        error!("Full compaction error: {}", e);
                    }
                }
            }
        });
    }
}
