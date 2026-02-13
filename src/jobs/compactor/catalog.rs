//! Catalog Operations for Compactor
//!
//! Handles catalog-level and table-level compaction.
//! Uses Object Store abstraction for both local and S3/MinIO storage.

use crate::storage::layout::STORE_DIR;
use futures::stream::{FuturesUnordered, StreamExt};
use tracing::{debug, warn};

use super::{CompactionConfig, CompactionResult, Compactor};

impl Compactor {
    /// Compact all tables in a catalog
    ///
    /// Runs tables in parallel (up to max_concurrent_compactions).
    /// Uses Object Store to discover schemas and tables.
    pub async fn compact_catalog(
        &self,
        catalog: &str,
        config: &CompactionConfig,
    ) -> Result<Vec<CompactionResult>, String> {
        // Phase 1: Collect all tables that need compaction using Object Store
        let mut tables_to_compact: Vec<(String, String)> = Vec::new();

        // List all schemas using Object Store
        let schemas = self.storage.list_prefixes(&format!("{}/{}", STORE_DIR, catalog)).await?;

        for schema_name in schemas {
            // Skip hidden directories and config files
            if schema_name.starts_with('.') || schema_name.ends_with(".json") {
                continue;
            }

            // List all tables in this schema using Object Store
            let schema_prefix = format!("{}/{}/{}", STORE_DIR, catalog, schema_name);
            let tables = match self.storage.list_prefixes(&schema_prefix).await {
                Ok(t) => t,
                Err(e) => {
                    warn!(
                        "Failed to list tables in schema {}/{}: {}",
                        catalog, schema_name, e
                    );
                    continue;
                }
            };

            for table_name in tables {
                tables_to_compact.push((schema_name.clone(), table_name));
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
    ///
    /// Runs sequentially within a table since most tables only have 1-2 active partitions.
    /// Uses Object Store to discover partitions.
    ///
    /// Path structure: catalog/schema/table/date=YYYY-MM-DD/node_id=X/*.parquet
    pub async fn compact_table_all_partitions(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        config: &CompactionConfig,
    ) -> Result<Vec<CompactionResult>, String> {
        let table_prefix = format!("{}/{}/{}/{}", STORE_DIR, catalog, schema, table);

        let mut results = Vec::new();

        // Check if table has partitions using Object Store
        let has_partitions = self.has_partitions_async(&table_prefix).await?;

        if has_partitions {
            // Partitioned table: compact each partition separately (sequentially)
            // List date= partitions using Object Store
            let partitions = self.storage.list_prefixes(&table_prefix).await?;

            for partition_name in partitions {
                // Check for Hive-style partition format: date=YYYY-MM-DD
                if partition_name.starts_with("date=") {
                    // Check for node_id= subdirectories under the date partition
                    let date_prefix = format!("{}/{}", table_prefix, partition_name);
                    let node_dirs = self
                        .storage
                        .list_prefixes(&date_prefix)
                        .await
                        .unwrap_or_default();

                    if node_dirs.iter().any(|d| d.starts_with("node_id=")) {
                        // New layout: compact each node_id subdirectory separately
                        for node_dir in &node_dirs {
                            if node_dir.starts_with("node_id=") {
                                let sub_partition =
                                    format!("{}/{}", partition_name, node_dir);
                                match self
                                    .compact_table_partition(
                                        catalog,
                                        schema,
                                        table,
                                        &sub_partition,
                                        config,
                                    )
                                    .await
                                {
                                    Ok(Some(result)) => results.push(result),
                                    Ok(None) => {}
                                    Err(e) => {
                                        warn!(
                                            "Failed to compact partition {}/{}/{}/{}: {}",
                                            catalog, schema, table, sub_partition, e
                                        );
                                    }
                                }
                            }
                        }
                    } else {
                        // Legacy layout or no node_id: compact at date level
                        match self
                            .compact_table_partition(
                                catalog, schema, table, &partition_name, config,
                            )
                            .await
                        {
                            Ok(Some(result)) => results.push(result),
                            Ok(None) => {}
                            Err(e) => {
                                warn!(
                                    "Failed to compact partition {}/{}/{}/{}: {}",
                                    catalog, schema, table, partition_name, e
                                );
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
    ///
    /// Uses Object Store to list prefixes.
    pub(crate) async fn has_partitions_async(&self, table_prefix: &str) -> Result<bool, String> {
        let prefixes = self.storage.list_prefixes(table_prefix).await?;

        for prefix in prefixes {
            if prefix.starts_with("date=") {
                return Ok(true);
            }
        }

        Ok(false)
    }
}
