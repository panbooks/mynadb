//! MynaDB-to-Iceberg Catalog Mapping
//!
//! Maps MynaDB's catalog/schema/table hierarchy to Iceberg namespaces:
//! - MynaDB catalog -> Iceberg warehouse
//! - MynaDB schema -> Iceberg namespace
//! - MynaDB table -> Iceberg table

use crate::iceberg::metadata::table_metadata::TableMetadata;
use crate::storage::layout::DataLayout;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, warn};

/// MynaDB-backed Iceberg catalog
///
/// Discovers tables by scanning MynaDB's data directory and
/// loads/generates Iceberg metadata for each table.
pub struct MynaDbIcebergCatalog {
    layout: DataLayout,
    /// Cache of loaded table metadata
    metadata_cache: parking_lot::RwLock<HashMap<(String, String, String), TableMetadata>>,
}

impl MynaDbIcebergCatalog {
    pub fn new(data_dir: &str) -> Self {
        Self {
            layout: DataLayout::new(data_dir),
            metadata_cache: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// List all namespaces (MynaDB schemas) for a catalog
    pub fn list_namespaces(&self, catalog: &str) -> Vec<String> {
        let catalog_dir = self.layout.catalog_dir(catalog);
        if !catalog_dir.exists() {
            return Vec::new();
        }

        let mut namespaces = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&catalog_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map_or(false, |t| t.is_dir()) {
                    if let Some(name) = entry.file_name().to_str() {
                        if !name.starts_with('_') && !name.starts_with('.') {
                            namespaces.push(name.to_string());
                        }
                    }
                }
            }
        }
        namespaces.sort();
        namespaces
    }

    /// List all tables in a namespace
    pub fn list_tables(&self, catalog: &str, namespace: &str) -> Vec<String> {
        let schema_dir = self.layout.schema_dir(catalog, namespace);
        if !schema_dir.exists() {
            return Vec::new();
        }

        let mut tables = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&schema_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map_or(false, |t| t.is_dir()) {
                    if let Some(name) = entry.file_name().to_str() {
                        if !name.starts_with('_') && !name.starts_with('.') {
                            tables.push(name.to_string());
                        }
                    }
                }
            }
        }
        tables.sort();
        tables
    }

    /// Check if a table exists
    pub fn table_exists(&self, catalog: &str, namespace: &str, table: &str) -> bool {
        self.layout.table_dir(catalog, namespace, table).exists()
    }

    /// Get table metadata (from cache or load from disk)
    pub fn get_table_metadata(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<TableMetadata> {
        let key = (
            catalog.to_string(),
            namespace.to_string(),
            table.to_string(),
        );

        // Check cache first
        {
            let cache = self.metadata_cache.read();
            if let Some(meta) = cache.get(&key) {
                return Some(meta.clone());
            }
        }

        // Try to load from iceberg metadata directory
        let metadata_dir = self.layout.iceberg_table_metadata_dir(catalog, namespace, table);
        let metadata_file = metadata_dir.join("v1.metadata.json");

        if metadata_file.exists() {
            match TableMetadata::load_json(&metadata_file) {
                Ok(meta) => {
                    let mut cache = self.metadata_cache.write();
                    cache.insert(key, meta.clone());
                    return Some(meta);
                }
                Err(e) => {
                    warn!(
                        "Failed to load Iceberg metadata for {}.{}.{}: {}",
                        catalog, namespace, table, e
                    );
                }
            }
        }

        None
    }

    /// Update cached metadata for a table
    pub fn update_metadata(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        metadata: TableMetadata,
    ) {
        let key = (
            catalog.to_string(),
            namespace.to_string(),
            table.to_string(),
        );
        let mut cache = self.metadata_cache.write();
        cache.insert(key, metadata);
    }

    /// Invalidate cached metadata for a table
    pub fn invalidate(&self, catalog: &str, namespace: &str, table: &str) {
        let key = (
            catalog.to_string(),
            namespace.to_string(),
            table.to_string(),
        );
        let mut cache = self.metadata_cache.write();
        cache.remove(&key);
    }

    /// Get the data layout
    pub fn layout(&self) -> &DataLayout {
        &self.layout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_creation() {
        let catalog = MynaDbIcebergCatalog::new("/tmp/test_iceberg");
        // Should not panic, just return empty lists
        assert!(catalog.list_namespaces("nonexistent").is_empty());
        assert!(catalog.list_tables("nonexistent", "default").is_empty());
        assert!(!catalog.table_exists("nonexistent", "default", "cpu"));
    }
}
