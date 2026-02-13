//! Iceberg Metadata Sync Job
//!
//! Background task that periodically scans MynaDB data directories
//! and generates/updates Iceberg metadata (snapshots, manifests, table metadata).

use super::snapshot_builder::SnapshotBuilder;
use crate::config::IcebergConfig;
use crate::iceberg::catalog::mynadb_catalog::MynaDbIcebergCatalog;
use crate::storage::layout::DataLayout;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

/// Background metadata sync job
pub struct MetadataSyncJob {
    config: IcebergConfig,
    catalog: Arc<MynaDbIcebergCatalog>,
    builder: SnapshotBuilder,
    data_dir: String,
}

impl MetadataSyncJob {
    pub fn new(
        config: IcebergConfig,
        catalog: Arc<MynaDbIcebergCatalog>,
        data_dir: &str,
    ) -> Self {
        Self {
            config,
            catalog,
            builder: SnapshotBuilder::new(data_dir),
            data_dir: data_dir.to_string(),
        }
    }

    /// Run a single sync cycle: scan all tables and update Iceberg metadata
    pub async fn sync_all(&self) -> Result<SyncStats, String> {
        let layout = DataLayout::new(&self.data_dir);
        let store_dir = layout.store_dir();

        if !store_dir.exists() {
            debug!("Store directory does not exist yet, skipping sync");
            return Ok(SyncStats::default());
        }

        let mut stats = SyncStats::default();

        // Scan all catalogs
        let catalogs = list_subdirs(&store_dir);
        for catalog in &catalogs {
            let namespaces = self.catalog.list_namespaces(catalog);
            for namespace in &namespaces {
                let tables = self.catalog.list_tables(catalog, namespace);
                for table in &tables {
                    match self.builder.build_table_snapshot(catalog, namespace, table) {
                        Ok(metadata) => {
                            self.catalog.update_metadata(catalog, namespace, table, metadata);
                            stats.tables_synced += 1;
                        }
                        Err(e) => {
                            warn!(
                                "Failed to sync Iceberg metadata for {}.{}.{}: {}",
                                catalog, namespace, table, e
                            );
                            stats.tables_failed += 1;
                        }
                    }
                }
            }
        }

        if stats.tables_synced > 0 {
            info!(
                "Iceberg metadata sync complete: {} tables synced, {} failed",
                stats.tables_synced, stats.tables_failed
            );
        }

        Ok(stats)
    }

    /// Sync a single table (called after flush or compaction)
    pub async fn sync_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<(), String> {
        match self.builder.build_table_snapshot(catalog, schema, table) {
            Ok(metadata) => {
                self.catalog.update_metadata(catalog, schema, table, metadata);
                debug!(
                    "Iceberg metadata synced for {}.{}.{}",
                    catalog, schema, table
                );
                Ok(())
            }
            Err(e) => {
                warn!(
                    "Failed to sync Iceberg metadata for {}.{}.{}: {}",
                    catalog, schema, table, e
                );
                Err(e)
            }
        }
    }

    /// Start background sync loop
    pub fn start_background(self: Arc<Self>) {
        let interval_secs = self.config.sync_interval_secs;
        if interval_secs == 0 {
            info!("Iceberg background sync disabled (interval=0)");
            return;
        }

        tokio::spawn(async move {
            info!(
                "Iceberg metadata sync started (interval: {}s)",
                interval_secs
            );

            let mut ticker = interval(Duration::from_secs(interval_secs));
            // Skip immediate first tick
            ticker.tick().await;

            loop {
                ticker.tick().await;
                match self.sync_all().await {
                    Ok(stats) => {
                        if stats.tables_synced > 0 || stats.tables_failed > 0 {
                            debug!(
                                "Iceberg sync: {} synced, {} failed",
                                stats.tables_synced, stats.tables_failed
                            );
                        }
                    }
                    Err(e) => {
                        error!("Iceberg sync error: {}", e);
                    }
                }
            }
        });
    }
}

/// Statistics from a sync run
#[derive(Debug, Default)]
pub struct SyncStats {
    pub tables_synced: usize,
    pub tables_failed: usize,
}

/// List subdirectories in a directory
fn list_subdirs(dir: &std::path::Path) -> Vec<String> {
    let mut dirs = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if entry.file_type().map_or(false, |t| t.is_dir()) {
                if let Some(name) = entry.file_name().to_str() {
                    if !name.starts_with('_') && !name.starts_with('.') {
                        dirs.push(name.to_string());
                    }
                }
            }
        }
    }
    dirs.sort();
    dirs
}
