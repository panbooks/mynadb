//! Table Registration Operations for QueryEngine
//!
//! Handles table registration with DataFusion, including partitioned tables,
//! memtable providers, and union providers.

use crate::query::providers::{
    MemtableProvider, MultiUnionTableProvider, ParquetTableConfig, ParquetTableProvider,
    UnionTableProvider,
};
use datafusion::catalog::TableProvider;
use datafusion::catalog_common::MemoryCatalogProvider;
use datafusion::catalog_common::MemorySchemaProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::discovery::TableLegacyPrefixes;
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
    pub async fn register_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<(), String> {
        // For object storage with memtable data, always re-register to include fresh writes
        // This is needed because the table might have been registered with stale/empty data
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
                    // For object storage with memtable data, re-register to include fresh writes
                    if is_object_storage && has_memtable_data {
                        debug!(
                            "Re-registering {}.{}.{} to include memtable data",
                            catalog_name, schema_name, table_name
                        );
                        // Deregister the old table first
                        let _ = schema.deregister_table(table_name);
                    } else {
                        // Table already registered and no new memtable data, skip
                        return Ok(());
                    }
                }
            }
        }

        // Build table URL based on storage backend
        // When write_cache is present, files are stored with node_id prefix
        let table_url = if let Some(ref storage) = self.storage {
            if let Some(ref cache) = self.write_cache {
                // With write_cache, always use node_id prefix (both local and object storage)
                storage.table_url_with_node(cache.node_id(), catalog_name, schema_name, table_name)
            } else {
                storage.table_url(catalog_name, schema_name, table_name)
            }
        } else {
            // No storage configured, use data_dir with node_id prefix if write_cache exists
            if let Some(ref cache) = self.write_cache {
                format!(
                    "file://{}/{}/{}/{}/{}",
                    self.data_dir,
                    cache.node_id(),
                    catalog_name,
                    schema_name,
                    table_name
                )
            } else {
                format!(
                    "file://{}/{}/{}/{}",
                    self.data_dir, catalog_name, schema_name, table_name
                )
            }
        };

        // Build local path for existence check (only for local storage)
        // Include node_id prefix if write_cache is present
        let table_path = if let Some(ref cache) = self.write_cache {
            format!(
                "{}/{}/{}/{}/{}",
                self.data_dir,
                cache.node_id(),
                catalog_name,
                schema_name,
                table_name
            )
        } else {
            format!(
                "{}/{}/{}/{}",
                self.data_dir, catalog_name, schema_name, table_name
            )
        };

        // For object storage with memtable data, use memtable-only registration
        // (fresh writes go to memtable before flush, parquet files don't exist yet)
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

        // Check cache first for table info
        let (has_partitions, partitions) = if let Some(cached) =
            self.metadata_cache
                .get_table_info(catalog_name, schema_name, table_name)
        {
            // Use cached partition info
            debug!(
                "Using cached table info for {}.{}.{}",
                catalog_name, schema_name, table_name
            );
            (cached.has_partitions, cached.partitions)
        } else {
            // Cache miss - need to scan directory
            if !path_exists {
                // Check if we have memtable data for this table
                if let Some(ref cache) = self.write_cache {
                    if cache.has_cached_data(catalog_name, schema_name, table_name) {
                        return self
                            .register_memtable_only(catalog_name, schema_name, table_name)
                            .await;
                    }
                }
                return Err(format!("Table path not found: {}", table_path));
            }

            // Discover partitions
            let partitions = if self.storage.is_none() || !is_object_storage {
                self.discover_table_partitions(&table_path)?
            } else {
                // For object storage, assume partitioned
                Vec::new()
            };

            let has_partitions = if self.storage.is_none() || !is_object_storage {
                !partitions.is_empty()
            } else {
                true
            };

            // Cache the result
            self.metadata_cache.set_table_info(
                catalog_name,
                schema_name,
                table_name,
                has_partitions,
                partitions.clone(),
            );
            debug!(
                "Cached table info for {}.{}.{} (partitions: {})",
                catalog_name,
                schema_name,
                table_name,
                partitions.len()
            );

            (has_partitions, partitions)
        };

        // Ensure catalog and schema exist
        self.ensure_schema_in_catalog(catalog_name, schema_name)
            .await?;

        // Try to register parquet table
        let parquet_result = if has_partitions {
            self.register_partitioned_table(catalog_name, schema_name, table_name, &table_url)
                .await
        } else {
            self.register_simple_table(catalog_name, schema_name, table_name, &table_url)
                .await
        };

        // If parquet registration fails (no files), try memtable-only for object storage
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
            // No memtable data either, return the original error
            return parquet_result;
        }

        if has_partitions {
            debug!(
                "Registered partitioned table: {}.{}.{} ({} partitions)",
                catalog_name,
                schema_name,
                table_name,
                partitions.len()
            );
        } else {
            debug!(
                "Registered non-partitioned table: {}.{}.{}",
                catalog_name, schema_name, table_name
            );
        }

        // Track registration
        let mut catalogs = self.registered_catalogs.write().await;
        catalogs
            .entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());

        Ok(())
    }

    /// Register a simple (non-partitioned) table - dispatches based on config
    async fn register_simple_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
    ) -> Result<(), String> {
        if self.use_custom_table_provider {
            self.register_table_custom(catalog_name, schema_name, table_name, table_path, false)
                .await
        } else {
            let table_ref =
                datafusion::sql::TableReference::full(catalog_name, schema_name, table_name);
            self.ctx
                .register_parquet(table_ref, table_path, ParquetReadOptions::default())
                .await
                .map_err(|e| {
                    format!(
                        "Failed to register table '{}.{}.{}': {}",
                        catalog_name, schema_name, table_name, e
                    )
                })
        }
    }

    /// Register a table using custom ParquetTableProvider (consolidated method)
    async fn register_table_custom(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
        partitioned: bool,
    ) -> Result<(), String> {
        let local_path = table_path.strip_prefix("file://").unwrap_or(table_path);
        let full_name = format!("{}.{}.{}", catalog_name, schema_name, table_name);

        let config = ParquetTableConfig::new(full_name.clone())
            .with_partitioned(partitioned)
            .with_batch_size(8192);

        let mut provider = ParquetTableProvider::try_new(local_path, config)
            .map_err(|e| format!("Failed to create table provider for '{}': {}", full_name, e))?;

        // Add parquet cache for cluster mode (object storage)
        if let Some(ref cache) = self.parquet_cache {
            provider = provider.with_parquet_cache(cache.clone());
        }

        self.register_provider(catalog_name, schema_name, table_name, Arc::new(provider))
    }

    /// Register a partitioned table - dispatches based on config
    async fn register_partitioned_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
    ) -> Result<(), String> {
        if self.use_custom_table_provider {
            self.register_table_custom(catalog_name, schema_name, table_name, table_path, true)
                .await
        } else {
            self.register_partitioned_table_listing(
                catalog_name,
                schema_name,
                table_name,
                table_path,
            )
            .await
        }
    }

    /// Register a partitioned table using DataFusion's ListingTable
    async fn register_partitioned_table_listing(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
    ) -> Result<(), String> {
        let table_url = ListingTableUrl::parse(table_path)
            .map_err(|e| format!("Failed to parse table URL: {}", e))?;

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![(
                "date".to_string(),
                datafusion::arrow::datatypes::DataType::Utf8,
            )]);

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .infer_schema(&self.ctx.state())
            .await
            .map_err(|e| format!("Failed to infer schema: {}", e))?;

        let listing_table = ListingTable::try_new(config)
            .map_err(|e| format!("Failed to create listing table: {}", e))?;

        self.register_provider(
            catalog_name,
            schema_name,
            table_name,
            Arc::new(listing_table),
        )
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

        let memtable_provider =
            match MemtableProvider::try_new(cache.clone(), catalog_name, schema_name, table_name) {
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

        let memtable_provider =
            MemtableProvider::try_new(cache.clone(), catalog_name, schema_name, table_name)
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

    // ========================================================================
    // Multi-Prefix Registration (Scale-In Support)
    // ========================================================================

    /// Register a table with legacy prefix support (for scale-in)
    ///
    /// When a cluster scales in, data from retired nodes remains in S3 under
    /// their original prefixes. This method creates a MultiUnionTableProvider
    /// that combines data from all relevant prefixes.
    pub async fn register_table_with_legacy(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        legacy: &TableLegacyPrefixes,
    ) -> Result<(), String> {
        // If no legacy prefixes, use normal registration
        if legacy.prefixes.is_empty() {
            return self.register_table(catalog_name, schema_name, table_name).await;
        }

        let is_object_storage = self
            .storage
            .as_ref()
            .map(|s| !s.is_local())
            .unwrap_or(false);

        // Multi-prefix registration only makes sense for object storage
        if !is_object_storage {
            debug!(
                "Local storage: ignoring legacy prefixes for {}.{}.{}",
                catalog_name, schema_name, table_name
            );
            return self.register_table(catalog_name, schema_name, table_name).await;
        }

        // Ensure catalog and schema exist
        self.ensure_schema_in_catalog(catalog_name, schema_name).await?;

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| "Storage not configured".to_string())?;

        // Build all table URLs (current node + legacy prefixes)
        let mut table_urls = Vec::new();
        let mut valid_prefixes = Vec::new();

        // Current node's URL
        if let Some(ref cache) = self.write_cache {
            let url = storage.table_url_with_node(
                cache.node_id(),
                catalog_name,
                schema_name,
                table_name,
            );
            table_urls.push((url, cache.node_id().to_string()));
        } else {
            let url = storage.table_url(catalog_name, schema_name, table_name);
            table_urls.push((url, "default".to_string()));
        }

        // Legacy prefix URLs
        for lp in &legacy.prefixes {
            let url = storage.table_url_with_node(lp, catalog_name, schema_name, table_name);
            table_urls.push((url, lp.clone()));
        }

        // Create providers for each valid URL
        let mut providers: Vec<Arc<dyn TableProvider>> = Vec::new();

        for (url, prefix) in table_urls {
            match self
                .create_listing_provider_for_url(&url, catalog_name, schema_name, table_name)
                .await
            {
                Ok(provider) => {
                    providers.push(provider);
                    valid_prefixes.push(prefix);
                }
                Err(e) => {
                    // Log but continue - some prefixes might have no data for this table
                    debug!(
                        "No data at prefix '{}' for {}.{}.{}: {}",
                        prefix, catalog_name, schema_name, table_name, e
                    );
                }
            }
        }

        if providers.is_empty() {
            // Check if we have memtable data
            if let Some(ref cache) = self.write_cache {
                if cache.has_cached_data(catalog_name, schema_name, table_name) {
                    return self
                        .register_memtable_only(catalog_name, schema_name, table_name)
                        .await;
                }
            }
            return Err(format!(
                "No data found for table {}.{}.{} in any prefix",
                catalog_name, schema_name, table_name
            ));
        }

        let full_name = format!("{}.{}.{}", catalog_name, schema_name, table_name);

        // If only one provider, use normal registration path
        if providers.len() == 1 {
            let provider = providers.remove(0);
            let final_provider =
                self.maybe_wrap_with_memtable(catalog_name, schema_name, table_name, provider)?;
            return self.register_provider(catalog_name, schema_name, table_name, final_provider);
        }

        // Create MultiUnionTableProvider for multiple prefixes
        let multi_union = MultiUnionTableProvider::try_new(providers, full_name.clone(), valid_prefixes.clone())
            .map_err(|e| format!("Failed to create multi-union provider: {}", e))?;

        // Wrap with memtable if needed
        let final_provider = self.maybe_wrap_with_memtable(
            catalog_name,
            schema_name,
            table_name,
            Arc::new(multi_union),
        )?;

        self.register_provider(catalog_name, schema_name, table_name, final_provider)?;

        info!(
            "Registered table {} with {} prefixes (including {} legacy): {:?}",
            full_name,
            valid_prefixes.len(),
            legacy.prefixes.len(),
            valid_prefixes
        );

        // Track registration
        let mut catalogs = self.registered_catalogs.write().await;
        catalogs
            .entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());

        Ok(())
    }

    /// Create a ListingTable provider for a specific URL
    async fn create_listing_provider_for_url(
        &self,
        table_url: &str,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn TableProvider>, String> {
        let listing_url = ListingTableUrl::parse(table_url)
            .map_err(|e| format!("Failed to parse table URL '{}': {}", table_url, e))?;

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![(
                "date".to_string(),
                datafusion::arrow::datatypes::DataType::Utf8,
            )]);

        let config = ListingTableConfig::new(listing_url)
            .with_listing_options(listing_options)
            .infer_schema(&self.ctx.state())
            .await
            .map_err(|e| {
                format!(
                    "Failed to infer schema for {}.{}.{} at {}: {}",
                    catalog_name, schema_name, table_name, table_url, e
                )
            })?;

        let listing_table = ListingTable::try_new(config)
            .map_err(|e| format!("Failed to create listing table: {}", e))?;

        Ok(Arc::new(listing_table))
    }
}
