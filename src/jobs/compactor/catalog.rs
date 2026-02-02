//! Catalog Operations for Compactor
//!
//! Handles catalog-level and table-level compaction.

use futures::stream::{FuturesUnordered, StreamExt};
use std::fs;
use std::path::Path;
use tracing::{debug, warn};

use super::{CompactionConfig, CompactionResult, Compactor};

impl Compactor {
    /// Compact all tables in a catalog
    /// Now runs tables in parallel (up to max_concurrent_compactions)
    pub async fn compact_catalog(
        &self,
        catalog: &str,
        config: &CompactionConfig,
    ) -> Result<Vec<CompactionResult>, String> {
        let catalog_path = format!("{}/{}", self.data_dir, catalog);

        if !Path::new(&catalog_path).exists() {
            return Err(format!("Catalog '{}' does not exist", catalog));
        }

        // Phase 1: Collect all tables that need compaction
        let mut tables_to_compact: Vec<(String, String)> = Vec::new();

        // List all schemas (subdirectories under catalog)
        let schema_entries = fs::read_dir(&catalog_path)
            .map_err(|e| format!("Failed to read catalog directory: {}", e))?;

        for schema_entry in schema_entries {
            let schema_entry =
                schema_entry.map_err(|e| format!("Failed to read schema entry: {}", e))?;
            let schema_path = schema_entry.path();

            // Skip non-directories and config files
            if !schema_path.is_dir() {
                continue;
            }

            let schema_name = match schema_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            // List all tables (subdirectories under schema)
            let table_entries = match fs::read_dir(&schema_path) {
                Ok(entries) => entries,
                Err(e) => {
                    warn!(
                        "Failed to read schema directory {}/{}: {}",
                        catalog, schema_name, e
                    );
                    continue;
                }
            };

            for table_entry in table_entries {
                let table_entry = match table_entry {
                    Ok(entry) => entry,
                    Err(e) => {
                        warn!("Failed to read table entry: {}", e);
                        continue;
                    }
                };
                let table_path = table_entry.path();

                // Skip non-directories
                if !table_path.is_dir() {
                    continue;
                }

                if let Some(table_name) = table_path.file_name().and_then(|n| n.to_str()) {
                    tables_to_compact.push((schema_name.clone(), table_name.to_string()));
                }
            }
        }

        if tables_to_compact.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            "Catalog {}: found {} tables for compaction (max {} concurrent)",
            catalog,
            tables_to_compact.len(),
            config.max_concurrent_compactions
        );

        // Phase 2: Process tables concurrently using FuturesUnordered
        let mut tasks = FuturesUnordered::new();

        for (schema_name, table_name) in tables_to_compact {
            // Clone strings to move into the future
            let catalog_owned = catalog.to_string();
            let schema_owned = schema_name.clone();
            let table_owned = table_name.clone();
            let config_clone = config.clone();

            let future = async move {
                self.compact_table_all_partitions(
                    &catalog_owned,
                    &schema_owned,
                    &table_owned,
                    &config_clone,
                )
                .await
            };
            tasks.push(future);
        }

        // Collect results as they complete
        let mut results = Vec::new();
        while let Some(result) = tasks.next().await {
            match result {
                Ok(table_results) => results.extend(table_results),
                Err(e) => {
                    warn!("Table compaction failed: {}", e);
                }
            }
        }

        Ok(results)
    }

    /// Compact all partitions in a table (or the table itself if not partitioned)
    /// Runs sequentially within a table since most tables only have 1-2 active partitions
    pub async fn compact_table_all_partitions(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        config: &CompactionConfig,
    ) -> Result<Vec<CompactionResult>, String> {
        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog, schema, table);

        if !Path::new(&table_path).exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        // Check if table has partitions or is a flat table
        let has_partitions = self.has_partitions(&table_path)?;

        if has_partitions {
            // Partitioned table: compact each partition separately (sequentially)
            // Most tables only have 1-2 active partitions (today + maybe yesterday)
            let entries = fs::read_dir(&table_path)
                .map_err(|e| format!("Failed to read table directory: {}", e))?;

            for entry in entries {
                let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
                let path = entry.path();

                if path.is_dir() {
                    if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                        // Check for Hive-style partition format: date=YYYY-MM-DD
                        if dir_name.starts_with("date=") {
                            match self
                                .compact_table_partition(catalog, schema, table, dir_name, config)
                                .await
                            {
                                Ok(Some(result)) => results.push(result),
                                Ok(None) => {
                                    // No compaction needed for this partition
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to compact partition {}/{}/{}/{}: {}",
                                        catalog, schema, table, dir_name, e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // Non-partitioned table: compact directly in table directory
            debug!(
                "Compacting non-partitioned table: {}.{}.{}",
                catalog, schema, table
            );
            match self
                .compact_non_partitioned_table(catalog, schema, table, config)
                .await
            {
                Ok(Some(result)) => results.push(result),
                Ok(None) => {
                    // No compaction needed
                }
                Err(e) => {
                    warn!(
                        "Failed to compact non-partitioned table {}/{}/{}: {}",
                        catalog, schema, table, e
                    );
                }
            }
        }

        Ok(results)
    }

    /// Check if a table has partitions (contains subdirectories with date= prefix)
    pub(crate) fn has_partitions(&self, table_path: &str) -> Result<bool, String> {
        let entries = fs::read_dir(table_path)
            .map_err(|e| format!("Failed to read table directory: {}", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                    if dir_name.starts_with("date=") {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }
}
