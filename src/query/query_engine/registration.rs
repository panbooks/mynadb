//! Table Registration Operations for QueryEngine
//!
//! Handles table registration with DataFusion, including partitioned tables,
//! memtable providers, and union providers.

use crate::query::providers::{
    MemtableProvider, ParquetTableConfig, ParquetTableProvider, UnionTableProvider,
};
use crate::storage::layout::STORE_DIR;
use datafusion::catalog::TableProvider;
use datafusion::catalog_common::MemoryCatalogProvider;
use datafusion::catalog_common::MemorySchemaProvider;
use datafusion::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::QueryEngine;

impl QueryEngine {
    /// Ensure catalog exists, create if needed
    pub(crate) async fn ensure_catalog(&self, catalog_name: &str) -> Result<(), String> {
        if self.ctx.catalog(catalog_name).is_none() {
            let catalog = Arc::new(MemoryCatalogProvider::new());
            self.ctx.register_catalog(catalog_name, catalog);
            info!("Created catalog: {}", catalog_name);
        }
        Ok(())
    }

    /// Ensure schema exists in catalog, create if needed
    pub(crate) async fn ensure_schema_in_catalog(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<(), String> {
        self.ensure_catalog(catalog_name).await?;

        let catalog = self
            .ctx
            .catalog(catalog_name)
            .ok_or_else(|| format!("Catalog '{}' not found", catalog_name))?;

        if catalog.schema(schema_name).is_none() {
            let schema = Arc::new(MemorySchemaProvider::new());
            catalog
                .register_schema(schema_name, schema)
                .map_err(|e| format!("Failed to register schema: {}", e))?;
            info!("Created schema: {}.{}", catalog_name, schema_name);
        }
        Ok(())
    }

    /// Register a table in catalog.schema.table format with partition support
    /// Uses cache to avoid re-scanning directories on every query
    /// Supports multi-prefix registration for scale-in (legacy prefixes from retired nodes)
    pub async fn register_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let is_object_storage = self
            .storage
            .as_ref()
            .map(|s| !s.is_local())
            .unwrap_or(false);

        let has_memtable_data = self
            .write_cache
            .as_ref()
            .map(|c| c.has_cached_data(catalog_name, schema_name, table_name))
            .unwrap_or(false);

        // Check if table is already registered in DataFusion
        if let Some(catalog) = self.ctx.catalog(catalog_name) {
            if let Some(schema) = catalog.schema(schema_name) {
                if schema.table_exist(table_name) {
                    // Re-register if we have new memtable data
                    if is_object_storage && has_memtable_data {
                        debug!(
                            "Re-registering {}.{}.{} (memtable={})",
                            catalog_name, schema_name, table_name, has_memtable_data
                        );
                        let _ = schema.deregister_table(table_name);
                    } else {
                        return Ok(());
                    }
                }
            }
        }

        // Build table URL based on storage backend
        // Note: With Hive partition structure (date=.../node_id=.../), we use the base table URL
        // without node_id prefix - node_id is discovered as a partition column
        let table_url = if let Some(ref storage) = self.storage {
            storage.table_url(catalog_name, schema_name, table_name)
        } else {
            format!(
                "file://{}/{}/{}/{}",
                self.data_dir, catalog_name, schema_name, table_name
            )
        };

        // Build local path for existence check
        // Note: With Hive partition structure, path is {data_dir}/{catalog}/{schema}/{table}/
        let table_path = format!(
            "{}/{}/{}/{}",
            self.data_dir, catalog_name, schema_name, table_name
        );

        // For object storage with memtable data, use memtable-only registration
        if is_object_storage && has_memtable_data {
            debug!(
                "Object storage: using memtable for {}.{}.{}",
                catalog_name, schema_name, table_name
            );
            return self
                .register_memtable_only(catalog_name, schema_name, table_name)
                .await;
        }

        // For local storage, check if path exists
        let path_exists = if self.storage.is_none() || !is_object_storage {
            std::path::Path::new(&table_path).exists()
        } else {
            true
        };

        // Check cache first for table info (all tables are partitioned by date)
        let partitions = if let Some(cached) =
            self.metadata_cache
                .get_table_info(catalog_name, schema_name, table_name)
        {
            debug!(
                "Using cached table info for {}.{}.{}",
                catalog_name, schema_name, table_name
            );
            cached.partitions
        } else {
            if !path_exists {
                if let Some(ref cache) = self.write_cache {
                    if cache.has_cached_data(catalog_name, schema_name, table_name) {
                        return self
                            .register_memtable_only(catalog_name, schema_name, table_name)
                            .await;
                    }
                }
                return Err(format!("Table path not found: {}", table_path));
            }

            let partitions = if self.storage.is_none() || !is_object_storage {
                self.discover_table_partitions(&table_path)?
            } else {
                Vec::new()
            };

            self.metadata_cache.set_table_info(
                catalog_name,
                schema_name,
                table_name,
                true, // All tables are partitioned
                partitions.clone(),
            );
            debug!(
                "Cached table info for {}.{}.{} (partitions: {})",
                catalog_name,
                schema_name,
                table_name,
                partitions.len()
            );

            partitions
        };

        self.ensure_schema_in_catalog(catalog_name, schema_name)
            .await?;

        // All tables are partitioned by date
        let parquet_result = self
            .register_parquet_table(catalog_name, schema_name, table_name, &table_url)
            .await;

        if parquet_result.is_err() {
            if let Some(ref cache) = self.write_cache {
                if cache.has_cached_data(catalog_name, schema_name, table_name) {
                    debug!(
                        "Parquet registration failed, falling back to memtable for {}.{}.{}",
                        catalog_name, schema_name, table_name
                    );
                    return self
                        .register_memtable_only(catalog_name, schema_name, table_name)
                        .await;
                }
            }
            return parquet_result;
        }

        debug!(
            "Registered table: {}.{}.{} ({} partitions)",
            catalog_name,
            schema_name,
            table_name,
            partitions.len()
        );

        let mut catalogs = self.registered_catalogs.write().await;
        catalogs
            .entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());

        Ok(())
    }

    /// Register a table using ParquetTableProvider
    ///
    /// All tables are partitioned by date (Hive-style: date=YYYY-MM-DD/node_id=X/).
    /// When Storage is available, uses ObjectStore for unified file discovery.
    /// Falls back to local filesystem operations when Storage is not configured.
    async fn register_parquet_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
    ) -> Result<(), String> {
        let full_name = format!("{}.{}.{}", catalog_name, schema_name, table_name);

        let config = ParquetTableConfig::new(table_name).with_batch_size(8192);

        // Use ObjectStore-based async constructor
        let mut provider = if let Some(ref storage) = self.storage {
            // Object store is rooted at raw data_dir, so paths need store/ prefix
            let base_path = format!("{}/{}/{}/{}", STORE_DIR, catalog_name, schema_name, table_name);

            ParquetTableProvider::try_new(storage.object_store(), base_path, catalog_name, schema_name, config)
                .await
                .map_err(|e| {
                    format!("Failed to create table provider for '{}': {}", full_name, e)
                })?
        } else {
            // Fallback: create local ObjectStore for local filesystem
            let local_path = table_path.strip_prefix("file://").unwrap_or(table_path);
            let local_store: std::sync::Arc<dyn object_store::ObjectStore> =
                std::sync::Arc::new(object_store::local::LocalFileSystem::new_with_prefix(local_path)
                    .map_err(|e| format!("Failed to create local store for '{}': {}", local_path, e))?);

            ParquetTableProvider::try_new(local_store, "", catalog_name, schema_name, config)
                .await
                .map_err(|e| {
                    format!("Failed to create table provider for '{}': {}", full_name, e)
                })?
        };

        // Add parquet cache for cluster mode
        if let Some(ref cache) = self.parquet_cache {
            provider = provider.with_parquet_cache(cache.clone());
        }

        // Add hot partition cache
        provider = provider.with_hot_partition_cache(self.hot_partition_cache.clone());

        self.register_provider(catalog_name, schema_name, table_name, Arc::new(provider))
    }

    /// Helper to register a TableProvider in the catalog
    pub(crate) fn register_provider(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        provider: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> Result<(), String> {
        let catalog = self
            .ctx
            .catalog(catalog_name)
            .ok_or_else(|| format!("Catalog '{}' not found", catalog_name))?;

        let schema = catalog
            .schema(schema_name)
            .ok_or_else(|| format!("Schema '{}.{}' not found", catalog_name, schema_name))?;

        let final_provider =
            self.maybe_wrap_with_memtable(catalog_name, schema_name, table_name, provider)?;

        schema
            .register_table(table_name.to_string(), final_provider)
            .map_err(|e| {
                format!(
                    "Failed to register table '{}.{}.{}': {}",
                    catalog_name, schema_name, table_name, e
                )
            })?;

        Ok(())
    }

    /// Wrap a Parquet table provider with memtable data if available
    fn maybe_wrap_with_memtable(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        parquet_provider: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>, String> {
        let cache = match &self.write_cache {
            Some(c) => c,
            None => return Ok(parquet_provider),
        };

        if !cache.has_cached_data(catalog_name, schema_name, table_name) {
            return Ok(parquet_provider);
        }

        let node_id = cache.node_id();
        let memtable_provider =
            match MemtableProvider::try_new(cache.clone(), catalog_name, schema_name, table_name, node_id) {
                Some(p) => Arc::new(p),
                None => return Ok(parquet_provider),
            };

        let full_name = format!("{}.{}.{}", catalog_name, schema_name, table_name);
        let union_provider =
            UnionTableProvider::new(memtable_provider, parquet_provider, full_name.clone())
                .map_err(|e| {
                    format!("Failed to create union provider for '{}': {}", full_name, e)
                })?;

        let stats = cache.get_table_cache_stats(catalog_name, schema_name, table_name);
        debug!(
            "Created memtable union for {}: {} cached rows in {} batches",
            full_name, stats.total_rows, stats.total_batches
        );

        Ok(Arc::new(union_provider))
    }

    /// Register a table with only Parquet data (skip memtable wrapping)
    ///
    /// Used for standby failover when querying a fallback Primary node.
    /// The fallback Primary doesn't have the memtable data (that's on the Standby),
    /// so we only want to query Parquet/S3 data.
    pub(crate) async fn register_parquet_only(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), String> {
        // Build table URL based on storage backend
        let table_url = if let Some(ref storage) = self.storage {
            storage.table_url(catalog_name, schema_name, table_name)
        } else {
            format!(
                "file://{}/{}/{}/{}",
                self.data_dir, catalog_name, schema_name, table_name
            )
        };

        // Build local path for existence check
        let table_path = format!(
            "{}/{}/{}/{}",
            self.data_dir, catalog_name, schema_name, table_name
        );

        // For local storage, check if path exists
        let is_object_storage = self
            .storage
            .as_ref()
            .map(|s| !s.is_local())
            .unwrap_or(false);

        let path_exists = if !is_object_storage {
            std::path::Path::new(&table_path).exists()
        } else {
            true // Object storage tables may not have local path
        };

        if !path_exists && !is_object_storage {
            return Err(format!("Table path not found: {}", table_path));
        }

        self.ensure_schema_in_catalog(catalog_name, schema_name)
            .await?;

        // Register Parquet table WITHOUT memtable wrapping
        self.register_parquet_table_no_memtable(catalog_name, schema_name, table_name, &table_url)
            .await?;

        debug!(
            "Registered parquet-only table: {}.{}.{} (no memtable)",
            catalog_name, schema_name, table_name
        );

        Ok(())
    }

    /// Register a Parquet table without memtable wrapping
    ///
    /// Used by `register_parquet_only` for standby failover queries.
    async fn register_parquet_table_no_memtable(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
    ) -> Result<(), String> {
        let config = ParquetTableConfig::new(table_name).with_batch_size(8192);

        let mut provider = if let Some(ref storage) = self.storage {
            let base_path = format!("{}/{}/{}/{}", STORE_DIR, catalog_name, schema_name, table_name);
            ParquetTableProvider::try_new(storage.object_store(), base_path, catalog_name, schema_name, config)
                .await
                .map_err(|e| format!("Failed to create table provider: {}", e))?
        } else {
            let local_path = table_path.strip_prefix("file://").unwrap_or(table_path);
            let local_store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::local::LocalFileSystem::new_with_prefix(local_path)
                    .map_err(|e| format!("Failed to create local store: {}", e))?);
            ParquetTableProvider::try_new(local_store, "", catalog_name, schema_name, config)
                .await
                .map_err(|e| format!("Failed to create table provider: {}", e))?
        };

        if let Some(ref cache) = self.parquet_cache {
            provider = provider.with_parquet_cache(cache.clone());
        }
        provider = provider.with_hot_partition_cache(self.hot_partition_cache.clone());

        // Register directly WITHOUT memtable wrapping
        let catalog = self
            .ctx
            .catalog(catalog_name)
            .ok_or_else(|| format!("Catalog '{}' not found", catalog_name))?;

        let schema = catalog
            .schema(schema_name)
            .ok_or_else(|| format!("Schema '{}.{}' not found", catalog_name, schema_name))?;

        schema
            .register_table(table_name.to_string(), Arc::new(provider))
            .map_err(|e| format!("Failed to register table: {}", e))?;

        Ok(())
    }

    /// Register a table with only memtable data (no Parquet files on disk)
    pub(crate) async fn register_memtable_only(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let cache = self
            .write_cache
            .as_ref()
            .ok_or_else(|| "Write cache not available".to_string())?;

        let node_id = cache.node_id();
        let memtable_provider =
            MemtableProvider::try_new(cache.clone(), catalog_name, schema_name, table_name, node_id)
                .ok_or_else(|| {
                    format!(
                        "No cached data available for {}.{}.{}",
                        catalog_name, schema_name, table_name
                    )
                })?;

        self.ensure_schema_in_catalog(catalog_name, schema_name)
            .await?;

        let full_name = format!("{}.{}.{}", catalog_name, schema_name, table_name);
        let stats = cache.get_table_cache_stats(catalog_name, schema_name, table_name);

        let catalog = self
            .ctx
            .catalog(catalog_name)
            .ok_or_else(|| format!("Catalog '{}' not found", catalog_name))?;

        let schema = catalog
            .schema(schema_name)
            .ok_or_else(|| format!("Schema '{}.{}' not found", catalog_name, schema_name))?;

        schema
            .register_table(table_name.to_string(), Arc::new(memtable_provider))
            .map_err(|e| {
                format!(
                    "Failed to register memtable '{}.{}.{}': {}",
                    catalog_name, schema_name, table_name, e
                )
            })?;

        debug!(
            "Registered memtable-only table {}: {} cached rows in {} batches",
            full_name, stats.total_rows, stats.total_batches
        );

        let mut catalogs = self.registered_catalogs.write().await;
        catalogs
            .entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());

        Ok(())
    }
}
