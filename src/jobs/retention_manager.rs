use super::database_manager::DatabaseManager;
use crate::config::RetentionConfig;
use crate::storage::storage::Storage;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// RetentionManager handles automatic deletion of old partitions
pub struct RetentionManager {
    db_manager: Arc<DatabaseManager>,
    data_dir: String,
    /// Storage backend (for future S3 support)
    #[allow(dead_code)]
    storage: Option<Arc<Storage>>,
}

impl RetentionManager {
    pub fn new(db_manager: Arc<DatabaseManager>, data_dir: &str) -> Self {
        RetentionManager {
            db_manager,
            data_dir: data_dir.to_string(),
            storage: None,
        }
    }

    pub fn with_storage(
        db_manager: Arc<DatabaseManager>,
        data_dir: &str,
        storage: Arc<Storage>,
    ) -> Self {
        RetentionManager {
            db_manager,
            data_dir: data_dir.to_string(),
            storage: Some(storage),
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

        // Discover all catalogs (databases)
        let catalogs = self.discover_catalogs()?;
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
        // Discover all schemas in this catalog
        let schemas = self.discover_schemas(catalog)?;

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
        // Discover all tables in this schema
        let tables = self.discover_tables(catalog, schema)?;

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
    async fn run_table_retention(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        config: &RetentionConfig,
        stats: &mut RetentionStats,
    ) -> Result<(), String> {
        // Delete partitions older than retention period
        let deleted = self.db_manager.delete_partitions_older_than_days(
            catalog,
            schema,
            table,
            config.default_days,
        )?;

        if !deleted.is_empty() {
            info!(
                "Deleted {} partition(s) from {}.{}.{}",
                deleted.len(),
                catalog,
                schema,
                table
            );

            for msg in deleted {
                debug!("  {}", msg);
                // Parse bytes from message (format: "... (N files, X.XX MB)")
                if let Some(size_mb) = extract_size_mb(&msg) {
                    stats.bytes_freed += (size_mb * 1024.0 * 1024.0) as u64;
                }
                stats.partitions_deleted += 1;
            }
        }

        Ok(())
    }

    /// Discover all catalogs (top-level directories in data_dir)
    fn discover_catalogs(&self) -> Result<Vec<String>, String> {
        let entries = std::fs::read_dir(&self.data_dir)
            .map_err(|e| format!("Failed to read data directory: {}", e))?;

        let mut catalogs = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    catalogs.push(name.to_string());
                }
            }
        }

        Ok(catalogs)
    }

    /// Discover all schemas in a catalog
    fn discover_schemas(&self, catalog: &str) -> Result<Vec<String>, String> {
        let catalog_path = std::path::Path::new(&self.data_dir).join(catalog);

        let entries = std::fs::read_dir(&catalog_path)
            .map_err(|e| format!("Failed to read catalog directory: {}", e))?;

        let mut schemas = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    // Skip config.json and other non-schema files
                    if name.ends_with(".json") || name.starts_with('.') {
                        continue;
                    }
                    schemas.push(name.to_string());
                }
            }
        }

        Ok(schemas)
    }

    /// Discover all tables in a schema
    fn discover_tables(&self, catalog: &str, schema: &str) -> Result<Vec<String>, String> {
        let schema_path = std::path::Path::new(&self.data_dir)
            .join(catalog)
            .join(schema);

        if !schema_path.exists() {
            return Ok(Vec::new());
        }

        let entries = std::fs::read_dir(&schema_path)
            .map_err(|e| format!("Failed to read schema directory: {}", e))?;

        let mut tables = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    tables.push(name.to_string());
                }
            }
        }

        Ok(tables)
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
