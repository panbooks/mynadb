//! Cache Operations for QueryEngine
//!
//! Handles metadata cache, query cache, and hot partition cache operations.

use crate::query::cache::hot_partition_cache::{HotPartitionCacheStats, SharedHotPartitionCache};
use crate::query::cache::metadata_cache::{CacheStatistics, TableMetadataCache};
use crate::query::cache::query_cache::{QueryCacheStats, SharedQueryResultCache};
use std::sync::Arc;
use tracing::{debug, warn};

use super::QueryEngine;

impl QueryEngine {
    /// Get reference to the metadata cache
    pub fn metadata_cache(&self) -> &Arc<TableMetadataCache> {
        &self.metadata_cache
    }

    /// Set the parquet local cache for cluster mode (object storage)
    /// This cache is used to download and cache parquet files locally
    pub fn set_parquet_cache(&mut self, cache: Arc<crate::data::cache::ParquetLocalCache>) {
        self.parquet_cache = Some(cache);
    }

    /// Get parquet cache reference
    pub fn parquet_cache(&self) -> Option<&Arc<crate::data::cache::ParquetLocalCache>> {
        self.parquet_cache.as_ref()
    }

    /// Get metadata cache statistics
    pub fn get_cache_stats(&self) -> CacheStatistics {
        self.metadata_cache.get_stats()
    }

    /// Get query result cache statistics
    pub fn get_query_cache_stats(&self) -> QueryCacheStats {
        self.query_cache.stats()
    }

    /// Get reference to the query result cache
    pub fn query_cache(&self) -> &SharedQueryResultCache {
        &self.query_cache
    }

    /// Get hot partition cache statistics
    pub fn get_hot_partition_cache_stats(&self) -> HotPartitionCacheStats {
        self.hot_partition_cache.stats()
    }

    /// Get reference to the hot partition cache
    pub fn hot_partition_cache(&self) -> &SharedHotPartitionCache {
        &self.hot_partition_cache
    }

    /// Invalidate cache for a table (call after writes)
    /// This invalidates metadata cache, query result cache, and hot partition cache
    pub fn invalidate_table_cache(&self, catalog: &str, schema: &str, table: &str) {
        self.metadata_cache.invalidate_table(catalog, schema, table);
        self.query_cache.invalidate_table(catalog, schema, table);
        self.hot_partition_cache
            .invalidate_table(catalog, schema, table);
    }

    /// Deregister a table from DataFusion session
    /// This is needed after flush so the table can be re-registered with proper providers
    /// (e.g., switching from memtable-only to parquet + memtable)
    pub fn deregister_table(&self, catalog: &str, schema: &str, table: &str) -> Result<(), String> {
        if let Some(catalog_provider) = self.ctx.catalog(catalog) {
            if let Some(schema_provider) = catalog_provider.schema(schema) {
                match schema_provider.deregister_table(table) {
                    Ok(Some(_)) => {
                        debug!(
                            "Deregistered table {}.{}.{} from DataFusion",
                            catalog, schema, table
                        );
                    }
                    Ok(None) => {
                        debug!("Table {}.{}.{} was not registered", catalog, schema, table);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to deregister table {}.{}.{}: {}",
                            catalog, schema, table, e
                        );
                        return Err(format!("Failed to deregister table: {}", e));
                    }
                }
            }
        }
        Ok(())
    }

    /// Refresh table registration after flush
    /// Deregisters the table and invalidates caches so it gets re-registered on next query
    pub fn refresh_table_after_flush(&self, catalog: &str, schema: &str, table: &str) {
        // First deregister the table from DataFusion
        if let Err(e) = self.deregister_table(catalog, schema, table) {
            warn!("Failed to deregister table during refresh: {}", e);
        }
        // Then invalidate all caches
        self.invalidate_table_cache(catalog, schema, table);
        debug!(
            "Refreshed table registration for {}.{}.{} after flush",
            catalog, schema, table
        );
    }

    /// Invalidate all caches for a database
    pub fn invalidate_database_cache(&self, db: &str) {
        self.metadata_cache.invalidate_catalog(db);
        self.query_cache.invalidate_database(db);
        self.hot_partition_cache.invalidate_catalog(db);
    }

    /// Clear all caches (metadata + query results + hot partitions)
    pub fn clear_all_caches(&self) {
        self.metadata_cache.clear();
        self.query_cache.clear();
        self.hot_partition_cache.clear();
    }

    /// Clear only query result cache
    pub fn clear_query_cache(&self) {
        self.query_cache.clear();
    }
}
