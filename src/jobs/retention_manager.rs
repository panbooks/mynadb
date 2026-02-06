use super::database_manager::DatabaseManager;
use crate::config::RetentionConfig;
use crate::meta::client::MetaClient;
use crate::storage::storage::Storage;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// RetentionManager handles automatic deletion of old partitions
///
/// Supports both standalone (local filesystem) and cluster (S3/MinIO) modes.
/// All storage operations use the Object Store abstraction.
///
/// In cluster mode, when all partitions for a table are deleted, it also
/// removes the table metadata from Meta to prevent unbounded growth of
/// the /tables/ prefix in Meta's storage.
pub struct RetentionManager {
    db_manager: Arc<DatabaseManager>,
    /// Storage backend for Object Store operations
    storage: Arc<Storage>,
    /// Optional Meta client for cluster mode table cleanup
    meta_client: Option<Arc<MetaClient>>,
}

impl RetentionManager {
    /// Create a new RetentionManager with storage backend
    ///
    /// All operations use the Object Store abstraction for both local and S3 storage.
    pub fn new(db_manager: Arc<DatabaseManager>, storage: Arc<Storage>) -> Self {
        RetentionManager {
            db_manager,
            storage,
            meta_client: None,
        }
    }

    /// Create a new RetentionManager with Meta client for cluster mode
    ///
    /// When Meta client is provided, the retention manager will also clean up
    /// table metadata from Meta when all partitions for a table are deleted.
    pub fn with_meta_client(
        db_manager: Arc<DatabaseManager>,
        storage: Arc<Storage>,
        meta_client: Arc<MetaClient>,
    ) -> Self {
        RetentionManager {
            db_manager,
            storage,
            meta_client: Some(meta_client),
        }
    }

    /// Start background retention task
    pub fn start_background_retention(self: Arc<Self>, config: RetentionConfig) {
        if !config.enabled {
            info!("Retention policy is disabled");
            return;
        }

        if config.interval_secs == 0 {
            warn!("Retention interval is 0, background retention disabled");
            return;
        }

        info!(
            "Starting background retention task (interval: {}s, retention: {} days)",
            config.interval_secs, config.default_days
        );

        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(config.interval_secs)).await;

                info!("Running retention check...");
                match self.run_retention_check(&config).await {
                    Ok(stats) => {
                        info!(
                            "Retention check completed: {} catalogs scanned, {} partitions deleted, {:.2} MB freed",
                            stats.catalogs_scanned,
                            stats.partitions_deleted,
                            stats.bytes_freed as f64 / 1024.0 / 1024.0
                        );
                    }
                    Err(e) => {
                        error!("Retention check failed: {}", e);
                    }
                }
            }
        });
    }

    /// Run a single retention check across all databases
    async fn run_retention_check(
        &self,
        config: &RetentionConfig,
    ) -> Result<RetentionStats, String> {
        let mut stats = RetentionStats::default();

        // Discover all catalogs (databases) using Object Store
        let catalogs = self.discover_catalogs().await?;
        stats.catalogs_scanned = catalogs.len();

        for catalog in catalogs {
            // Skip excluded databases
            if config.exclude_databases.contains(&catalog) {
                debug!("Skipping excluded catalog: {}", catalog);
                continue;
            }

            debug!("Checking retention for catalog: {}", catalog);
            match self
                .run_catalog_retention(&catalog, config, &mut stats)
                .await
            {
                Ok(()) => {
                    debug!("Retention check completed for catalog: {}", catalog);
                }
                Err(e) => {
                    warn!("Failed to run retention for catalog '{}': {}", catalog, e);
                }
            }
        }

        Ok(stats)
    }

    /// Run retention check for a single catalog
    async fn run_catalog_retention(
        &self,
        catalog: &str,
        config: &RetentionConfig,
        stats: &mut RetentionStats,
    ) -> Result<(), String> {
        // Discover all schemas in this catalog using Object Store
        let schemas = self.discover_schemas(catalog).await?;

        for schema in schemas {
            debug!("Checking retention for schema: {}.{}", catalog, schema);
            match self
                .run_schema_retention(catalog, &schema, config, stats)
                .await
            {
                Ok(()) => {
                    debug!(
                        "Retention check completed for schema: {}.{}",
                        catalog, schema
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to run retention for schema '{}.{}': {}",
                        catalog, schema, e
                    );
                }
            }
        }

        Ok(())
    }

    /// Run retention check for a single schema
    async fn run_schema_retention(
        &self,
        catalog: &str,
        schema: &str,
        config: &RetentionConfig,
        stats: &mut RetentionStats,
    ) -> Result<(), String> {
        // Discover all tables in this schema using Object Store
        let tables = self.discover_tables(catalog, schema).await?;

        for table in tables {
            let full_table_name = format!("{}.{}.{}", catalog, schema, table);

            // Skip excluded tables
            if config.exclude_tables.contains(&full_table_name) {
                debug!("Skipping excluded table: {}", full_table_name);
                continue;
            }

            debug!("Running retention for table: {}", full_table_name);
            match self
                .run_table_retention(catalog, schema, &table, config, stats)
                .await
            {
                Ok(()) => {
                    debug!("Retention check completed for table: {}", full_table_name);
                }
                Err(e) => {
                    warn!(
                        "Failed to run retention for table '{}': {}",
                        full_table_name, e
                    );
                }
            }
        }

        Ok(())
    }

    /// Run retention check for a single table
    ///
    /// Uses Object Store to discover and delete old partitions.
    /// Handles the new path structure: catalog/schema/table/date=YYYY-MM-DD/node_id=X/
    async fn run_table_retention(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        config: &RetentionConfig,
        stats: &mut RetentionStats,
    ) -> Result<(), String> {
        use chrono::{Duration, Utc};

        let cutoff_date = Utc::now() - Duration::days(config.default_days as i64);
        let cutoff_str = cutoff_date.format("%Y-%m-%d").to_string();

        // Discover partitions using Object Store
        let partitions = self
            .db_manager
            .discover_table_partitions_async(catalog, schema, table)
            .await?;

        let mut deleted_count = 0;
        for partition in partitions {
            // Check if partition is older than cutoff
            if partition.date.as_str() < cutoff_str.as_str() {
                // Delete the partition using Object Store
                match self
                    .delete_partition_async(catalog, schema, table, &partition.partition)
                    .await
                {
                    Ok(bytes_freed) => {
                        debug!(
                            "Deleted partition {}.{}.{}/{} ({} files, {:.2} MB)",
                            catalog,
                            schema,
                            table,
                            partition.partition,
                            partition.file_count,
                            bytes_freed as f64 / 1024.0 / 1024.0
                        );
                        stats.bytes_freed += bytes_freed;
                        stats.partitions_deleted += 1;
                        deleted_count += 1;
                    }
                    Err(e) => {
                        warn!(
                            "Failed to delete partition {}.{}.{}/{}: {}",
                            catalog, schema, table, partition.partition, e
                        );
                    }
                }
            }
        }

        if deleted_count > 0 {
            info!(
                "Deleted {} partition(s) from {}.{}.{}",
                deleted_count, catalog, schema, table
            );

            // Check if all partitions are deleted (table is now empty)
            // Only do this check if we deleted something, to avoid unnecessary calls
            let remaining_partitions = self
                .db_manager
                .discover_table_partitions_async(catalog, schema, table)
                .await
                .unwrap_or_default();

            if remaining_partitions.is_empty() {
                info!(
                    "All partitions deleted from {}.{}.{}, cleaning up table metadata",
                    catalog, schema, table
                );

                // Clean up table metadata from Meta (cluster mode only)
                if let Some(ref meta_client) = self.meta_client {
                    match meta_client.remove_table(catalog, schema, table).await {
                        Ok(()) => {
                            info!(
                                "Removed empty table {}.{}.{} from Meta",
                                catalog, schema, table
                            );
                        }
                        Err(e) => {
                            // Log but don't fail - table may already be removed or
                            // Meta might be temporarily unavailable
                            warn!(
                                "Failed to remove empty table {}.{}.{} from Meta: {}",
                                catalog, schema, table, e
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Delete a partition using Object Store batch deletion
    ///
    /// Path structure: catalog/schema/table/date=YYYY-MM-DD/node_id=X/*.parquet
    async fn delete_partition_async(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        partition: &str,
    ) -> Result<u64, String> {
        // Ensure partition format: date=YYYY-MM-DD
        let partition_dir = if partition.starts_with("date=") {
            partition.to_string()
        } else {
            format!("date={}", partition)
        };

        let partition_prefix = format!("{}/{}/{}/{}", catalog, schema, table, partition_dir);

        // Delete all files under the partition prefix (includes node_id= subdirectories)
        let result = self.storage.delete_prefix(&partition_prefix).await?;

        debug!(
            "Deleted {} files ({:.2} MB) from {}",
            result.files_deleted,
            result.bytes_freed as f64 / 1024.0 / 1024.0,
            partition_prefix
        );

        Ok(result.bytes_freed)
    }

    /// Discover all catalogs (top-level directories) using Object Store
    async fn discover_catalogs(&self) -> Result<Vec<String>, String> {
        // Use Object Store list_prefixes to get top-level directories
        self.storage.list_prefixes("").await
    }

    /// Discover all schemas in a catalog using Object Store
    async fn discover_schemas(&self, catalog: &str) -> Result<Vec<String>, String> {
        // Use Object Store list_prefixes to get schema directories
        let schemas = self.storage.list_prefixes(catalog).await?;

        // Filter out config files and hidden directories
        Ok(schemas
            .into_iter()
            .filter(|name| !name.ends_with(".json") && !name.starts_with('.'))
            .collect())
    }

    /// Discover all tables in a schema using Object Store
    async fn discover_tables(&self, catalog: &str, schema: &str) -> Result<Vec<String>, String> {
        let prefix = format!("{}/{}", catalog, schema);
        self.storage.list_prefixes(&prefix).await
    }
}

/// Statistics from a retention check
#[derive(Debug, Default)]
struct RetentionStats {
    catalogs_scanned: usize,
    partitions_deleted: usize,
    bytes_freed: u64,
}

/// Extract size in MB from deletion message
/// Message format: "Deleted partition ... (N files, X.XX MB)"
fn extract_size_mb(msg: &str) -> Option<f64> {
    // Find the pattern "X.XX MB)" or "X MB)"
    if let Some(mb_pos) = msg.rfind(" MB)") {
        // Find the last space before " MB)"
        let before_mb = &msg[..mb_pos];
        if let Some(last_space) = before_mb.rfind(' ') {
            let size_str = &before_mb[last_space + 1..];
            return size_str.parse::<f64>().ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_size_mb() {
        let msg = "Deleted partition mydb.default.cpu/date=2024-01-01 (5 files, 123.45 MB)";
        assert_eq!(extract_size_mb(msg), Some(123.45));

        let msg2 = "Deleted partition mydb.default.cpu/date=2024-01-02 (2 files, 50.00 MB)";
        assert_eq!(extract_size_mb(msg2), Some(50.00));

        let msg3 = "Deleted partition mydb.default.cpu/date=2024-01-03 (1 files, 5 MB)";
        assert_eq!(extract_size_mb(msg3), Some(5.0));
    }
}
