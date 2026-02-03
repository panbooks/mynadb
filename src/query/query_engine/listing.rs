//! Listing Operations for QueryEngine
//!
//! Handles table and schema listing operations.

use std::collections::HashSet;
use tracing::debug;

use super::discovery::TableLegacyPrefixes;
use super::QueryEngine;

impl QueryEngine {
    // ========================================================================
    // Schema & Table Listing
    // ========================================================================

    /// List all registered tables across all catalogs
    /// Note: This includes tables registered by queries even if they have no data
    pub async fn list_tables(&self) -> Vec<String> {
        let mut all_tables = Vec::new();

        for catalog_name in self.ctx.catalog_names() {
            if let Some(catalog) = self.ctx.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if let Some(schema) = catalog.schema(&schema_name) {
                        for table_name in schema.table_names() {
                            all_tables
                                .push(format!("{}.{}.{}", catalog_name, schema_name, table_name));
                        }
                    }
                }
            }
        }

        all_tables.sort();
        all_tables
    }

    /// List tables in a catalog that have actual data (from storage or cache)
    /// This is more expensive than list_tables() but only returns tables with data
    pub async fn list_tables_with_data(&self, catalog_name: &str) -> Vec<String> {
        let mut tables: HashSet<String> = HashSet::new();

        // Get tables from in-memory cache
        if let Some(ref cache) = self.write_cache {
            for (schema, table) in cache.list_cached_tables_in_catalog(catalog_name) {
                tables.insert(format!("{}.{}.{}", catalog_name, schema, table));
            }
        }

        // Get tables from storage
        if let Some(ref storage) = self.storage {
            // Build prefix based on whether we have node_id
            let prefix = if let Some(ref cache) = self.write_cache {
                format!("{}/{}/", cache.node_id(), catalog_name)
            } else {
                format!("{}/", catalog_name)
            };

            // List files from storage
            if let Ok(files) = storage.list(&prefix).await {
                for file in files {
                    // Extract schema/table from path like: {schema}/{table}/date=YYYY-MM-DD/file.parquet
                    let relative = file.strip_prefix(&prefix).unwrap_or(&file);
                    let parts: Vec<&str> = relative.split('/').collect();
                    if parts.len() >= 2 {
                        let schema_name = parts[0];
                        let table_name = parts[1];
                        if !schema_name.is_empty()
                            && !table_name.is_empty()
                            && !schema_name.starts_with('.')
                            && !table_name.starts_with('.')
                            && !table_name.starts_with("date=")
                        {
                            tables.insert(format!(
                                "{}.{}.{}",
                                catalog_name, schema_name, table_name
                            ));
                        }
                    }
                }
            }
        } else {
            // Local filesystem
            let catalog_path = std::path::Path::new(&self.data_dir).join(catalog_name);
            let schema_entries = match std::fs::read_dir(&catalog_path) {
                Ok(entries) => entries,
                Err(_) => return Vec::new(),
            };

            for schema_entry in schema_entries.flatten() {
                let schema_path = schema_entry.path();
                if !schema_path.is_dir() {
                    continue;
                }

                let schema_name = match schema_entry.file_name().to_str() {
                    Some(name) if !name.starts_with('.') => name.to_string(),
                    _ => continue,
                };

                let table_entries = match std::fs::read_dir(&schema_path) {
                    Ok(entries) => entries,
                    Err(_) => continue,
                };

                for table_entry in table_entries.flatten() {
                    if !table_entry.path().is_dir() {
                        continue;
                    }

                    let table_name = match table_entry.file_name().to_str() {
                        Some(name) if !name.starts_with('.') => name.to_string(),
                        _ => continue,
                    };

                    tables.insert(format!("{}.{}.{}", catalog_name, schema_name, table_name));
                }
            }
        }

        let mut result: Vec<String> = tables.into_iter().collect();
        result.sort();
        result
    }

    /// Get schema for a table (supports both qualified "db.table" and unqualified "table")
    pub async fn get_table_schema(&self, table_name: &str) -> Result<String, String> {
        let table = self
            .ctx
            .table(table_name)
            .await
            .map_err(|e| format!("Table not found: {}", e))?;

        let schema = table.schema();
        Ok(format!("{:#?}", schema))
    }

    /// List all schemas (services) in a catalog
    pub async fn list_schemas(&self, catalog_name: &str) -> Result<Vec<String>, String> {
        let mut schemas_set: HashSet<String> = HashSet::new();

        // Get schemas from in-memory cache
        if let Some(ref cache) = self.write_cache {
            for (schema, _table) in cache.list_cached_tables_in_catalog(catalog_name) {
                schemas_set.insert(schema);
            }
        }

        // Get schemas from storage
        if let Some(ref storage) = self.storage {
            // Build prefix based on whether we have node_id
            let prefix = if let Some(ref cache) = self.write_cache {
                format!("{}/{}/", cache.node_id(), catalog_name)
            } else {
                format!("{}/", catalog_name)
            };

            // List files from storage and extract schemas
            if let Ok(files) = storage.list(&prefix).await {
                for file in files {
                    // Extract schema from path like: {schema}/{table}/date=YYYY-MM-DD/file.parquet
                    let relative = file.strip_prefix(&prefix).unwrap_or(&file);
                    let parts: Vec<&str> = relative.split('/').collect();
                    if !parts.is_empty() {
                        let schema_name = parts[0];
                        if !schema_name.is_empty()
                            && !schema_name.starts_with('.')
                            && schema_name != "_system"
                            && schema_name != "_wal"
                        {
                            schemas_set.insert(schema_name.to_string());
                        }
                    }
                }
            }
        } else {
            // Fallback to local filesystem for standalone mode
            let catalog_path = format!("{}/{}", self.data_dir, catalog_name);

            if std::path::Path::new(&catalog_path).exists() {
                if let Ok(entries) = std::fs::read_dir(&catalog_path) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            if let Some(schema_name) = path.file_name().and_then(|s| s.to_str()) {
                                if !schema_name.starts_with('.') && schema_name != "_system" {
                                    schemas_set.insert(schema_name.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut schemas: Vec<String> = schemas_set.into_iter().collect();
        schemas.sort();
        Ok(schemas)
    }

    /// List all tables in a catalog.schema
    pub async fn list_tables_in_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<String>, String> {
        let mut tables_set: HashSet<String> = HashSet::new();

        // Get tables from in-memory cache
        if let Some(ref cache) = self.write_cache {
            for (schema, table) in cache.list_cached_tables_in_catalog(catalog_name) {
                if schema == schema_name {
                    tables_set.insert(table);
                }
            }
        }

        // Get tables from storage
        if let Some(ref storage) = self.storage {
            // Build prefix based on whether we have node_id
            let prefix = if let Some(ref cache) = self.write_cache {
                format!("{}/{}/{}/", cache.node_id(), catalog_name, schema_name)
            } else {
                format!("{}/{}/", catalog_name, schema_name)
            };

            // List files from storage and extract table names
            if let Ok(files) = storage.list(&prefix).await {
                for file in files {
                    // Extract table from path like: {table}/date=YYYY-MM-DD/file.parquet
                    let relative = file.strip_prefix(&prefix).unwrap_or(&file);
                    let parts: Vec<&str> = relative.split('/').collect();
                    if !parts.is_empty() {
                        let table_name = parts[0];
                        if !table_name.is_empty()
                            && !table_name.starts_with('.')
                            && !table_name.starts_with("date=")
                        {
                            tables_set.insert(table_name.to_string());
                        }
                    }
                }
            }
        } else {
            // Fallback to local filesystem for standalone mode
            let schema_path = format!("{}/{}/{}", self.data_dir, catalog_name, schema_name);

            if std::path::Path::new(&schema_path).exists() {
                if let Ok(entries) = std::fs::read_dir(&schema_path) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            if let Some(table_name) = path.file_name().and_then(|s| s.to_str()) {
                                if !table_name.starts_with('.') {
                                    tables_set.insert(table_name.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut tables: Vec<String> = tables_set.into_iter().collect();
        tables.sort();
        Ok(tables)
    }

    // ========================================================================
    // Multi-Prefix Listing (Scale-In Support)
    // ========================================================================

    /// List tables in a catalog with data, scanning all prefixes (current + legacy)
    pub async fn list_tables_with_data_and_legacy(
        &self,
        catalog_name: &str,
        legacy: &TableLegacyPrefixes,
    ) -> Vec<String> {
        let mut tables: HashSet<String> = HashSet::new();

        // Get tables from in-memory cache
        if let Some(ref cache) = self.write_cache {
            for (schema, table) in cache.list_cached_tables_in_catalog(catalog_name) {
                tables.insert(format!("{}.{}.{}", catalog_name, schema, table));
            }
        }

        // Get tables from storage (all prefixes)
        if let Some(ref storage) = self.storage {
            // Build all prefixes to scan
            let mut all_prefixes = Vec::new();
            if let Some(ref cache) = self.write_cache {
                all_prefixes.push(format!("{}/{}/", cache.node_id(), catalog_name));
            } else {
                all_prefixes.push(format!("{}/", catalog_name));
            }
            for lp in &legacy.prefixes {
                all_prefixes.push(format!("{}/{}/", lp, catalog_name));
            }

            for prefix in &all_prefixes {
                if let Ok(files) = storage.list(prefix).await {
                    for file in files {
                        let relative = file.strip_prefix(prefix).unwrap_or(&file);
                        let parts: Vec<&str> = relative.split('/').collect();
                        if parts.len() >= 2 {
                            let schema_name = parts[0];
                            let table_name = parts[1];
                            if !schema_name.is_empty()
                                && !table_name.is_empty()
                                && !schema_name.starts_with('.')
                                && !table_name.starts_with('.')
                                && !table_name.starts_with("date=")
                            {
                                tables.insert(format!(
                                    "{}.{}.{}",
                                    catalog_name, schema_name, table_name
                                ));
                            }
                        }
                    }
                }
            }
        } else {
            // Local filesystem - no legacy prefix support
            return self.list_tables_with_data(catalog_name).await;
        }

        let mut result: Vec<String> = tables.into_iter().collect();
        result.sort();
        result
    }

    /// List all schemas in a catalog, scanning all prefixes (current + legacy)
    pub async fn list_schemas_with_legacy(
        &self,
        catalog_name: &str,
        legacy: &TableLegacyPrefixes,
    ) -> Result<Vec<String>, String> {
        let mut schemas_set: HashSet<String> = HashSet::new();

        // Get schemas from in-memory cache
        if let Some(ref cache) = self.write_cache {
            for (schema, _table) in cache.list_cached_tables_in_catalog(catalog_name) {
                schemas_set.insert(schema);
            }
        }

        // Get schemas from storage (all prefixes)
        if let Some(ref storage) = self.storage {
            // Build all prefixes to scan
            let mut all_prefixes = Vec::new();
            if let Some(ref cache) = self.write_cache {
                all_prefixes.push(format!("{}/{}/", cache.node_id(), catalog_name));
            } else {
                all_prefixes.push(format!("{}/", catalog_name));
            }
            for lp in &legacy.prefixes {
                all_prefixes.push(format!("{}/{}/", lp, catalog_name));
            }

            for prefix in &all_prefixes {
                if let Ok(files) = storage.list(prefix).await {
                    for file in files {
                        let relative = file.strip_prefix(prefix).unwrap_or(&file);
                        let parts: Vec<&str> = relative.split('/').collect();
                        if !parts.is_empty() {
                            let schema_name = parts[0];
                            if !schema_name.is_empty()
                                && !schema_name.starts_with('.')
                                && schema_name != "_system"
                                && schema_name != "_wal"
                            {
                                schemas_set.insert(schema_name.to_string());
                            }
                        }
                    }
                }
            }
        } else {
            // Local filesystem - no legacy prefix support
            return self.list_schemas(catalog_name).await;
        }

        let mut schemas: Vec<String> = schemas_set.into_iter().collect();
        schemas.sort();
        Ok(schemas)
    }

    /// List all tables in a catalog.schema, scanning all prefixes (current + legacy)
    pub async fn list_tables_in_schema_with_legacy(
        &self,
        catalog_name: &str,
        schema_name: &str,
        legacy: &TableLegacyPrefixes,
    ) -> Result<Vec<String>, String> {
        let mut tables_set: HashSet<String> = HashSet::new();

        // Get tables from in-memory cache
        if let Some(ref cache) = self.write_cache {
            for (schema, table) in cache.list_cached_tables_in_catalog(catalog_name) {
                if schema == schema_name {
                    tables_set.insert(table);
                }
            }
        }

        // Get tables from storage (all prefixes)
        if let Some(ref storage) = self.storage {
            // Build all prefixes to scan
            let mut all_prefixes = Vec::new();
            if let Some(ref cache) = self.write_cache {
                all_prefixes.push(format!("{}/{}/{}/", cache.node_id(), catalog_name, schema_name));
            } else {
                all_prefixes.push(format!("{}/{}/", catalog_name, schema_name));
            }
            for lp in &legacy.prefixes {
                all_prefixes.push(format!("{}/{}/{}/", lp, catalog_name, schema_name));
            }

            for prefix in &all_prefixes {
                if let Ok(files) = storage.list(prefix).await {
                    for file in files {
                        let relative = file.strip_prefix(prefix).unwrap_or(&file);
                        let parts: Vec<&str> = relative.split('/').collect();
                        if !parts.is_empty() {
                            let table_name = parts[0];
                            if !table_name.is_empty()
                                && !table_name.starts_with('.')
                                && !table_name.starts_with("date=")
                            {
                                tables_set.insert(table_name.to_string());
                            }
                        }
                    }
                }
            }
        } else {
            // Local filesystem - no legacy prefix support
            return self.list_tables_in_schema(catalog_name, schema_name).await;
        }

        let mut tables: Vec<String> = tables_set.into_iter().collect();
        tables.sort();
        Ok(tables)
    }
}

// Suppress unused warning for debug macro
#[allow(unused_imports)]
use tracing as _;
