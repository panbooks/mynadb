use crate::storage::Storage;
use datafusion::arrow::array::RecordBatch;
use datafusion::catalog_common::MemoryCatalogProvider;
use datafusion::catalog_common::MemorySchemaProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

const DEFAULT_SCHEMA: &str = "default";

pub struct QueryEngine {
    ctx: SessionContext,
    data_dir: String,
    storage: Option<Arc<Storage>>,
    // Track registered catalogs (databases) and their schemas (services)
    registered_catalogs: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl QueryEngine {
    pub fn new(data_dir: &str) -> Self {
        let ctx = SessionContext::new();
        QueryEngine {
            ctx,
            data_dir: data_dir.to_string(),
            storage: None,
            registered_catalogs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn new_with_storage(data_dir: &str, storage: Arc<Storage>) -> Self {
        let ctx = SessionContext::new();

        // Register object store with DataFusion runtime
        if !storage.is_local() {
            let url = url::Url::parse(storage.url_prefix())
                .expect("Failed to parse storage URL prefix");
            ctx.runtime_env()
                .register_object_store(&url, storage.object_store());
            info!("Registered object store with DataFusion: {}", storage.url_prefix());
        }

        QueryEngine {
            ctx,
            data_dir: data_dir.to_string(),
            storage: Some(storage),
            registered_catalogs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Ensure catalog exists, create if needed
    async fn ensure_catalog(&self, catalog_name: &str) -> Result<(), String> {
        if self.ctx.catalog(catalog_name).is_none() {
            let catalog = Arc::new(MemoryCatalogProvider::new());
            self.ctx.register_catalog(catalog_name, catalog);
            info!("Created catalog: {}", catalog_name);
        }
        Ok(())
    }

    /// Ensure schema exists in catalog, create if needed
    async fn ensure_schema_in_catalog(
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
    pub async fn register_table(
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

        // Build local path for existence check (only for local storage)
        let table_path = format!(
            "{}/{}/{}/{}",
            self.data_dir, catalog_name, schema_name, table_name
        );

        // For local storage, check if path exists
        if self.storage.is_none() || self.storage.as_ref().map(|s| s.is_local()).unwrap_or(true) {
            if !std::path::Path::new(&table_path).exists() {
                return Err(format!("Table path not found: {}", table_path));
            }
        }

        // Ensure catalog and schema exist
        self.ensure_schema_in_catalog(catalog_name, schema_name)
            .await?;

        // Check if table is already registered
        if let Some(catalog) = self.ctx.catalog(catalog_name) {
            if let Some(schema) = catalog.schema(schema_name) {
                if schema.table_exist(table_name) {
                    // Table already registered, skip
                    return Ok(());
                }
            }
        }

        // Check if table has Hive-style partitions (for local storage)
        let has_partitions = if self.storage.is_none() || self.storage.as_ref().map(|s| s.is_local()).unwrap_or(true) {
            let partitions = self.discover_table_partitions(&table_path)?;
            !partitions.is_empty()
        } else {
            // For object storage, assume partitioned
            true
        };

        if has_partitions {
            // Register partitioned table using ListingTable API
            self.register_partitioned_table(catalog_name, schema_name, table_name, &table_url)
                .await?;
            debug!(
                "Registered partitioned table: {}.{}.{}",
                catalog_name, schema_name, table_name
            );
        } else {
            // No partitions, use standard registration
            self.register_simple_table(catalog_name, schema_name, table_name, &table_url)
                .await?;
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

    /// Register a simple (non-partitioned) table
    async fn register_simple_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
    ) -> Result<(), String> {
        let table_ref = TableReference::full(catalog_name, schema_name, table_name);
        self.ctx
            .register_parquet(table_ref, table_path, ParquetReadOptions::default())
            .await
            .map_err(|e| {
                format!(
                    "Failed to register table '{}.{}.{}': {}",
                    catalog_name, schema_name, table_name, e
                )
            })?;
        Ok(())
    }

    /// Register a partitioned table using ListingTable
    async fn register_partitioned_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
    ) -> Result<(), String> {
        // Create ListingTableUrl from the table path
        let table_url = ListingTableUrl::parse(table_path)
            .map_err(|e| format!("Failed to parse table URL: {}", e))?;

        // Create ListingOptions for Parquet with Hive partitioning
        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format)
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![(
                "date".to_string(),
                datafusion::arrow::datatypes::DataType::Utf8,
            )]); // Partition column

        // Create ListingTableConfig
        let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);

        // Infer schema from the files
        let config = config
            .infer_schema(&self.ctx.state())
            .await
            .map_err(|e| format!("Failed to infer schema: {}", e))?;

        // Create the ListingTable
        let listing_table = ListingTable::try_new(config)
            .map_err(|e| format!("Failed to create listing table: {}", e))?;

        // Get the catalog and schema
        let catalog = self
            .ctx
            .catalog(catalog_name)
            .ok_or_else(|| format!("Catalog '{}' not found", catalog_name))?;

        let schema = catalog
            .schema(schema_name)
            .ok_or_else(|| format!("Schema '{}.{}' not found", catalog_name, schema_name))?;

        // Register the table
        schema
            .register_table(table_name.to_string(), Arc::new(listing_table))
            .map_err(|e| format!("Failed to register table: {}", e))?;

        Ok(())
    }

    /// Discover date partitions in a table directory
    fn discover_table_partitions(&self, table_path: &str) -> Result<Vec<String>, String> {
        let path = std::path::Path::new(table_path);
        if !path.exists() {
            return Ok(Vec::new());
        }

        let entries = std::fs::read_dir(path)
            .map_err(|e| format!("Failed to read table directory: {}", e))?;

        let mut partitions = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|s| s.to_str()) {
                    // Check for Hive-style partition format: date=YYYY-MM-DD
                    if dir_name.starts_with("date=") {
                        partitions.push(dir_name.to_string());
                    }
                }
            }
        }

        partitions.sort();
        Ok(partitions)
    }

    /// Auto-discover and register all tables in catalog.schema from disk
    pub async fn discover_and_register(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<usize, String> {
        let schema_path = format!("{}/{}/{}", self.data_dir, catalog_name, schema_name);

        if !std::path::Path::new(&schema_path).exists() {
            return Ok(0);
        }

        let entries = std::fs::read_dir(&schema_path)
            .map_err(|e| format!("Failed to read schema directory: {}", e))?;

        let mut count = 0;
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(table_name) = path.file_name().and_then(|s| s.to_str()) {
                    if !table_name.starts_with('.') {
                        if let Err(e) = self
                            .register_table(catalog_name, schema_name, table_name)
                            .await
                        {
                            warn!(
                                "Failed to register {}.{}.{}: {}",
                                catalog_name, schema_name, table_name, e
                            );
                        } else {
                            count += 1;
                        }
                    }
                }
            }
        }

        Ok(count)
    }

    /// Auto-discover all schemas in a catalog
    pub async fn discover_catalog(&self, catalog_name: &str) -> Result<usize, String> {
        let catalog_path = format!("{}/{}", self.data_dir, catalog_name);

        if !std::path::Path::new(&catalog_path).exists() {
            return Ok(0);
        }

        let entries = std::fs::read_dir(&catalog_path)
            .map_err(|e| format!("Failed to read catalog directory: {}", e))?;

        let mut total_tables = 0;
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(schema_name) = path.file_name().and_then(|s| s.to_str()) {
                    if !schema_name.starts_with('.') {
                        match self.discover_and_register(catalog_name, schema_name).await {
                            Ok(count) => total_tables += count,
                            Err(e) => warn!(
                                "Failed to discover schema {}.{}: {}",
                                catalog_name, schema_name, e
                            ),
                        }
                    }
                }
            }
        }

        Ok(total_tables)
    }

    /// Ensure schema exists (creates path if needed for write operations)
    pub async fn ensure_schema(&self, catalog_name: &str, schema_name: &str) -> Result<(), String> {
        // Just track it - actual registration happens when tables are written
        let mut catalogs = self.registered_catalogs.write().await;
        catalogs
            .entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());
        Ok(())
    }

    /// Legacy compatibility: ensure database registered (now auto-discovers all schemas)
    pub async fn ensure_database_registered(&self, db_name: &str) -> Result<(), String> {
        let count = self.discover_catalog(db_name).await?;
        if count > 0 {
            info!(
                "Auto-registered {} table(s) in catalog '{}'",
                count, db_name
            );
        }
        Ok(())
    }

    /// Execute a SQL query with database context
    /// Automatically qualifies unqualified table names with catalog.schema
    pub async fn execute_query_with_db(
        &self,
        sql: &str,
        db_name: &str,
    ) -> Result<Vec<RecordBatch>, String> {
        // Extract table references from SQL and register only those tables
        let table_refs = self.extract_table_references(sql, db_name);

        for (catalog, schema, table) in table_refs {
            // Try to register the table (will skip if already registered)
            // Ignore errors here - DataFusion will report if table doesn't exist
            let _ = self.register_table(&catalog, &schema, &table).await;
        }

        // Qualify unqualified table names with catalog.schema
        let qualified_sql = self.qualify_table_names(sql, db_name);

        debug!("SQL: {}", sql);
        if qualified_sql != sql {
            debug!("Qualified: {}", qualified_sql);
        }

        self.execute_query(&qualified_sql).await
    }

    /// Execute a SQL query (without DB context)
    pub async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>, String> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Execution error: {}", e))?;

        Ok(batches)
    }

    /// Get the schema for a SQL query without executing it
    pub async fn get_query_schema(
        &self,
        db_name: &str,
        sql: &str,
    ) -> Result<Arc<datafusion::arrow::datatypes::Schema>, String> {
        // Ensure database is registered
        self.ensure_database_registered(db_name).await?;

        // Qualify table names
        let qualified_sql = self.qualify_table_names(sql, db_name);

        let df = self
            .ctx
            .sql(&qualified_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;

        Ok(df.schema().inner().clone())
    }

    /// Parse a table reference considering quoted identifiers
    /// Returns (parts, original_string)
    fn parse_table_reference(table_ref: &str) -> (Vec<String>, String) {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut quote_char = ' ';
        let chars: Vec<char> = table_ref.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            let ch = chars[i];

            if !in_quotes && (ch == '"' || ch == '`') {
                in_quotes = true;
                quote_char = ch;
                i += 1;
                continue;
            }

            if in_quotes && ch == quote_char {
                in_quotes = false;
                // Add the current quoted part
                if !current.is_empty() {
                    parts.push(current.clone());
                    current.clear();
                }
                i += 1;
                // Skip the dot after closing quote if present
                if i < chars.len() && chars[i] == '.' {
                    i += 1;
                }
                continue;
            }

            if !in_quotes && ch == '.' {
                if !current.is_empty() {
                    parts.push(current.clone());
                    current.clear();
                }
                i += 1;
                continue;
            }

            current.push(ch);
            i += 1;
        }

        if !current.is_empty() {
            parts.push(current);
        }

        (parts, table_ref.to_string())
    }

    /// Quote an identifier if it contains special characters or mixed case
    /// DataFusion requires quotes for:
    /// - Case-sensitive identifiers (any uppercase letters)
    /// - Identifiers with special characters (dots, hyphens, spaces)
    fn quote_identifier(ident: &str) -> String {
        let needs_quoting = ident.contains('.')
            || ident.contains('-')
            || ident.contains(' ')
            || ident.chars().any(|c| c.is_uppercase()); // Preserve case-sensitive names

        if needs_quoting {
            format!("\"{}\"", ident)
        } else {
            ident.to_string()
        }
    }

    /// Extract table references from SQL query
    /// Returns Vec<(catalog, schema, table)> with fully resolved names
    fn extract_table_references(&self, sql: &str, catalog_name: &str) -> Vec<(String, String, String)> {
        let sql_upper = sql.to_uppercase();
        let mut table_refs = Vec::new();

        let keywords = [
            "FROM",
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
        ];

        for keyword in &keywords {
            let pattern = format!("{} ", keyword);
            let mut search_pos = 0;

            while let Some(pos) = sql_upper[search_pos..].find(&pattern) {
                let abs_pos = search_pos + pos + pattern.len();
                let remaining = &sql[abs_pos..];

                // Extract all comma-separated table references
                let mut table_list_end = 0;
                let mut in_quotes = false;
                let mut quote_char = ' ';
                let mut paren_depth = 0;

                // Find the end of the table list (stops at WHERE, ORDER BY, GROUP BY, etc.)
                for (i, ch) in remaining.chars().enumerate() {
                    if !in_quotes && ch == '(' {
                        paren_depth += 1;
                    } else if !in_quotes && ch == ')' {
                        paren_depth -= 1;
                        if paren_depth < 0 {
                            table_list_end = i;
                            break;
                        }
                    } else if !in_quotes && (ch == '"' || ch == '`') {
                        in_quotes = true;
                        quote_char = ch;
                    } else if in_quotes && ch == quote_char {
                        in_quotes = false;
                    } else if !in_quotes && paren_depth == 0 {
                        // Check for SQL keywords that end the table list
                        let remaining_upper = &sql_upper[abs_pos + i..];
                        if remaining_upper.starts_with("WHERE ")
                            || remaining_upper.starts_with("GROUP ")
                            || remaining_upper.starts_with("ORDER ")
                            || remaining_upper.starts_with("LIMIT ")
                            || remaining_upper.starts_with("HAVING ")
                            || remaining_upper.starts_with("UNION ")
                            || remaining_upper.starts_with("EXCEPT ")
                            || remaining_upper.starts_with("INTERSECT ")
                            || ch == ';'
                        {
                            table_list_end = i;
                            break;
                        }
                    }
                }

                if table_list_end == 0 {
                    table_list_end = remaining.len();
                }

                let table_list = &remaining[..table_list_end];

                // Split by commas and process each table reference
                let mut current_table = String::new();
                let mut in_quotes = false;
                let mut quote_char = ' ';

                for ch in table_list.chars() {
                    if !in_quotes && (ch == '"' || ch == '`') {
                        in_quotes = true;
                        quote_char = ch;
                        current_table.push(ch);
                    } else if in_quotes && ch == quote_char {
                        in_quotes = false;
                        current_table.push(ch);
                    } else if !in_quotes && ch == ',' {
                        // Process the current table
                        self.process_table_ref(&current_table, catalog_name, &mut table_refs);
                        current_table.clear();
                    } else {
                        current_table.push(ch);
                    }
                }

                // Process the last table
                if !current_table.is_empty() {
                    self.process_table_ref(&current_table, catalog_name, &mut table_refs);
                }

                search_pos = abs_pos + table_list_end;
            }
        }

        // Deduplicate
        table_refs.sort();
        table_refs.dedup();
        table_refs
    }

    /// Helper to process a single table reference and add to the list
    fn process_table_ref(&self, table_ref: &str, catalog_name: &str, table_refs: &mut Vec<(String, String, String)>) {
        let table_ref = table_ref.trim();

        // Skip empty refs, subqueries, or aliases (words after whitespace)
        if table_ref.is_empty() || table_ref.starts_with('(') {
            return;
        }

        // Remove alias if present (e.g., "cpu c" -> "cpu")
        let table_ref = table_ref.split_whitespace().next().unwrap_or(table_ref);

        // Parse the table reference considering quotes
        let (parts, _) = Self::parse_table_reference(table_ref);

        // Determine catalog, schema, table based on number of parts
        let (catalog, schema, table) = match parts.len() {
            0 => return,
            1 => {
                // Simple name: cpu -> catalog.default.cpu
                (catalog_name.to_string(), DEFAULT_SCHEMA.to_string(), parts[0].clone())
            }
            2 => {
                // Partially qualified: schema.table -> catalog.schema.table
                (catalog_name.to_string(), parts[0].clone(), parts[1].clone())
            }
            _ => {
                // Fully qualified: catalog.schema.table
                (parts[0].clone(), parts[1].clone(), parts[2].clone())
            }
        };

        table_refs.push((catalog, schema, table));
    }

    /// Qualify unqualified or partially qualified table names with catalog.schema
    ///
    /// Examples (with catalog_name = "testdb"):
    /// - "cpu" -> "testdb.default.cpu"
    /// - "webapp.cpu" -> "testdb.webapp.cpu"
    /// - "testdb.webapp.cpu" -> "testdb.webapp.cpu" (already fully qualified)
    /// - "otherdb.schema.table" -> "otherdb.schema.table" (different catalog, unchanged)
    /// - "default.\"cat.transaction\"" -> "testdb.default.\"cat.transaction\""
    fn qualify_table_names(&self, sql: &str, catalog_name: &str) -> String {
        let sql_upper = sql.to_uppercase();
        let mut result = sql.to_string();
        let mut offset: i64 = 0; // Track the offset due to replacements

        let keywords = [
            "FROM",
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
        ];

        for keyword in &keywords {
            let pattern = format!("{} ", keyword);
            let mut search_pos = 0;

            while let Some(pos) = sql_upper[search_pos..].find(&pattern) {
                let abs_pos = search_pos + pos + pattern.len();
                let remaining = &sql[abs_pos..];

                // Find end of table reference, considering quotes
                let mut end_pos = 0;
                let mut in_quotes = false;
                let mut quote_char = ' ';

                for (i, ch) in remaining.chars().enumerate() {
                    if !in_quotes && (ch == '"' || ch == '`') {
                        in_quotes = true;
                        quote_char = ch;
                    } else if in_quotes && ch == quote_char {
                        in_quotes = false;
                    } else if !in_quotes
                        && (ch.is_whitespace() || ch == ',' || ch == ';' || ch == ')')
                    {
                        end_pos = i;
                        break;
                    }
                }

                if end_pos == 0 {
                    end_pos = remaining.len();
                }

                let table_ref = remaining[..end_pos].trim();

                // Skip empty refs or subqueries
                if table_ref.is_empty() || table_ref.starts_with('(') {
                    search_pos = abs_pos + end_pos;
                    continue;
                }

                // Parse the table reference considering quotes
                let (parts, _) = Self::parse_table_reference(table_ref);

                // Determine qualification based on number of parts
                let qualified = match parts.len() {
                    0 => {
                        search_pos = abs_pos + end_pos;
                        continue;
                    }
                    1 => {
                        // Simple name: cpu -> catalog.default.cpu
                        format!(
                            "{}.{}.{}",
                            catalog_name,
                            DEFAULT_SCHEMA,
                            Self::quote_identifier(&parts[0])
                        )
                    }
                    2 => {
                        // Partially qualified: schema.table -> catalog.schema.table
                        format!(
                            "{}.{}.{}",
                            catalog_name,
                            Self::quote_identifier(&parts[0]),
                            Self::quote_identifier(&parts[1])
                        )
                    }
                    _ => {
                        // Already fully qualified (catalog.schema.table), don't modify
                        // But reconstruct it with proper quoting
                        format!(
                            "{}.{}.{}",
                            Self::quote_identifier(&parts[0]),
                            Self::quote_identifier(&parts[1]),
                            Self::quote_identifier(&parts[2])
                        )
                    }
                };

                // Only replace if we actually changed something
                if qualified != table_ref {
                    // Calculate the actual position in the result string with offset
                    let result_pos = (abs_pos as i64 + offset) as usize;
                    let before_part = &result[..result_pos];
                    let after_part = &result[result_pos + table_ref.len()..];

                    result = format!("{}{}{}", before_part, qualified, after_part);

                    // Update offset
                    offset += qualified.len() as i64 - table_ref.len() as i64;
                }

                search_pos = abs_pos + end_pos;
            }
        }

        result
    }

    /// Execute query and return as JSON (with DB context)
    pub async fn execute_query_json_with_db(
        &self,
        sql: &str,
        db_name: &str,
    ) -> Result<String, String> {
        let batches = self.execute_query_with_db(sql, db_name).await?;

        if batches.is_empty() {
            return Ok("[]".to_string());
        }

        // Convert to JSON
        let mut results = Vec::new();
        for batch in batches {
            let json_rows = Self::batch_to_json(&batch)?;
            results.extend(json_rows);
        }

        serde_json::to_string_pretty(&results)
            .map_err(|e| format!("JSON serialization error: {}", e))
    }

    /// Convert RecordBatch to JSON objects
    fn batch_to_json(batch: &RecordBatch) -> Result<Vec<serde_json::Value>, String> {
        let mut rows = Vec::new();
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        for row_idx in 0..num_rows {
            let mut row_obj = serde_json::Map::new();

            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_idx);
                let value = Self::array_value_to_json(column, row_idx)?;
                row_obj.insert(field.name().clone(), value);
            }

            rows.push(serde_json::Value::Object(row_obj));
        }

        Ok(rows)
    }

    /// Convert Arrow array value to JSON
    fn array_value_to_json(
        array: &Arc<dyn datafusion::arrow::array::Array>,
        index: usize,
    ) -> Result<serde_json::Value, String> {
        use datafusion::arrow::array::*;
        use datafusion::arrow::datatypes::{DataType, UInt16Type};

        if array.is_null(index) {
            return Ok(serde_json::Value::Null);
        }

        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Utf8View => {
                let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Timestamp(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                Ok(serde_json::json!(arr.value(index)))
            }
            DataType::Dictionary(_, value_type) => {
                // Handle dictionary-encoded strings (used for partition columns)
                match value_type.as_ref() {
                    DataType::Utf8 => {
                        let dict_array = array
                            .as_any()
                            .downcast_ref::<DictionaryArray<UInt16Type>>()
                            .unwrap();
                        let keys = dict_array.keys();
                        let key = keys.value(index);
                        let values = dict_array
                            .values()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        Ok(serde_json::json!(values.value(key as usize)))
                    }
                    _ => Ok(serde_json::json!(format!(
                        "Unsupported dictionary value type: {:?}",
                        value_type
                    ))),
                }
            }
            _ => Ok(serde_json::json!(format!(
                "Unsupported type: {:?}",
                array.data_type()
            ))),
        }
    }

    /// List all registered tables across all catalogs
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
        let catalog_path = format!("{}/{}", self.data_dir, catalog_name);

        if !std::path::Path::new(&catalog_path).exists() {
            return Ok(Vec::new());
        }

        let entries = std::fs::read_dir(&catalog_path)
            .map_err(|e| format!("Failed to read catalog directory: {}", e))?;

        let mut schemas = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(schema_name) = path.file_name().and_then(|s| s.to_str()) {
                    if !schema_name.starts_with('.') && schema_name != "_system" {
                        schemas.push(schema_name.to_string());
                    }
                }
            }
        }

        schemas.sort();
        Ok(schemas)
    }

    /// List all tables in a catalog.schema
    pub async fn list_tables_in_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<String>, String> {
        let schema_path = format!("{}/{}/{}", self.data_dir, catalog_name, schema_name);

        if !std::path::Path::new(&schema_path).exists() {
            return Ok(Vec::new());
        }

        let entries = std::fs::read_dir(&schema_path)
            .map_err(|e| format!("Failed to read schema directory: {}", e))?;

        let mut tables = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(table_name) = path.file_name().and_then(|s| s.to_str()) {
                    if !table_name.starts_with('.') {
                        tables.push(table_name.to_string());
                    }
                }
            }
        }

        tables.sort();
        Ok(tables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_query_engine() {
        let _engine = QueryEngine::new("./data");
        // Add tests here
    }

    #[test]
    fn test_qualify_table_names_unqualified() {
        let engine = QueryEngine::new("./data");

        // Unqualified table name: cpu -> testdb.default.cpu
        let sql = "SELECT * FROM cpu WHERE usage > 50";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM testdb.default.cpu WHERE usage > 50");
    }

    #[test]
    fn test_qualify_table_names_partially_qualified() {
        let engine = QueryEngine::new("./data");

        // Partially qualified: webapp.cpu -> testdb.webapp.cpu
        let sql = "SELECT * FROM webapp.cpu LIMIT 10";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM testdb.webapp.cpu LIMIT 10");
    }

    #[test]
    fn test_qualify_table_names_fully_qualified() {
        let engine = QueryEngine::new("./data");

        // Fully qualified: testdb.webapp.cpu -> testdb.webapp.cpu (unchanged)
        let sql = "SELECT * FROM testdb.webapp.cpu WHERE timestamp > 0";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(
            result,
            "SELECT * FROM testdb.webapp.cpu WHERE timestamp > 0"
        );
    }

    #[test]
    fn test_qualify_table_names_different_catalog() {
        let engine = QueryEngine::new("./data");

        // Different catalog: otherdb.schema.table -> otherdb.schema.table (unchanged)
        let sql = "SELECT * FROM otherdb.webapp.cpu";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM otherdb.webapp.cpu");
    }

    #[test]
    fn test_qualify_table_names_multiple_tables() {
        let engine = QueryEngine::new("./data");

        // Multiple tables with different qualification levels
        let sql = "SELECT * FROM cpu JOIN webapp.memory ON cpu.hostname = memory.hostname";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM testdb.default.cpu JOIN testdb.webapp.memory ON cpu.hostname = memory.hostname");
    }

    #[test]
    fn test_qualify_table_names_with_alias() {
        let engine = QueryEngine::new("./data");

        // Table with alias
        let sql = "SELECT c.* FROM cpu c WHERE c.usage > 50";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(
            result,
            "SELECT c.* FROM testdb.default.cpu c WHERE c.usage > 50"
        );
    }

    #[test]
    fn test_qualify_table_names_case_insensitive() {
        let engine = QueryEngine::new("./data");

        // Test case insensitivity (FROM, from, FrOm should all work)
        let sql1 = "select * from cpu";
        let sql2 = "SELECT * FROM cpu";
        let sql3 = "SeLeCt * FrOm cpu";

        let result1 = engine.qualify_table_names(sql1, "testdb");
        let result2 = engine.qualify_table_names(sql2, "testdb");
        let result3 = engine.qualify_table_names(sql3, "testdb");

        assert_eq!(result1, "select * from testdb.default.cpu");
        assert_eq!(result2, "SELECT * FROM testdb.default.cpu");
        assert_eq!(result3, "SeLeCt * FrOm testdb.default.cpu");
    }

    #[test]
    fn test_qualify_table_names_with_subquery() {
        let engine = QueryEngine::new("./data");

        // Subqueries - inner tables should be qualified
        let sql = "SELECT * FROM (SELECT * FROM cpu) AS subquery";
        let result = engine.qualify_table_names(sql, "testdb");
        // The inner FROM should be qualified
        assert!(result.contains("FROM testdb.default.cpu"));
    }

    #[test]
    fn test_qualify_table_names_comma_separated() {
        let engine = QueryEngine::new("./data");

        // Comma-separated tables (old-style join syntax)
        let sql = "SELECT * FROM cpu, memory WHERE cpu.hostname = memory.hostname";
        let result = engine.qualify_table_names(sql, "testdb");
        // Only the first table after FROM is qualified; comma-separated tables need special handling
        assert!(result.contains("testdb.default.cpu"));
    }

    #[test]
    fn test_qualify_table_names_with_quoted_identifiers() {
        let engine = QueryEngine::new("./data");

        // Quoted identifier with dot inside
        let sql = r#"SELECT * FROM testdb.default."cat.transaction""#;
        let result = engine.qualify_table_names(sql, "testdb");
        // Should remain unchanged as it's fully qualified
        assert_eq!(result, r#"SELECT * FROM testdb.default."cat.transaction""#);

        // Partially qualified with quoted identifier
        let sql2 = r#"SELECT * FROM default."cat.transaction""#;
        let result2 = engine.qualify_table_names(sql2, "testdb");
        assert_eq!(result2, r#"SELECT * FROM testdb.default."cat.transaction""#);

        // Unqualified quoted identifier
        let sql3 = r#"SELECT * FROM "cat.transaction""#;
        let result3 = engine.qualify_table_names(sql3, "testdb");
        assert_eq!(result3, r#"SELECT * FROM testdb.default."cat.transaction""#);
    }

    #[test]
    fn test_parse_table_reference() {
        // Simple name
        let (parts, _) = QueryEngine::parse_table_reference("cpu");
        assert_eq!(parts, vec!["cpu"]);

        // Schema.table
        let (parts, _) = QueryEngine::parse_table_reference("webapp.cpu");
        assert_eq!(parts, vec!["webapp", "cpu"]);

        // Catalog.schema.table
        let (parts, _) = QueryEngine::parse_table_reference("testdb.webapp.cpu");
        assert_eq!(parts, vec!["testdb", "webapp", "cpu"]);

        // Quoted identifier with dot
        let (parts, _) = QueryEngine::parse_table_reference(r#""cat.transaction""#);
        assert_eq!(parts, vec!["cat.transaction"]);

        // Schema with quoted table
        let (parts, _) = QueryEngine::parse_table_reference(r#"default."cat.transaction""#);
        assert_eq!(parts, vec!["default", "cat.transaction"]);

        // Fully qualified with quoted table
        let (parts, _) = QueryEngine::parse_table_reference(r#"testdb.default."cat.transaction""#);
        assert_eq!(parts, vec!["testdb", "default", "cat.transaction"]);
    }

    #[test]
    fn test_quote_identifier_case_sensitive() {
        // Case-sensitive identifiers (with uppercase) should be quoted
        assert_eq!(
            QueryEngine::quote_identifier("TestDiskUsage"),
            r#""TestDiskUsage""#
        );
        assert_eq!(
            QueryEngine::quote_identifier("Executor.ActiveThread"),
            r#""Executor.ActiveThread""#
        );

        // All lowercase - no quotes needed
        assert_eq!(QueryEngine::quote_identifier("cpu"), "cpu");

        // Special chars - needs quotes
        assert_eq!(QueryEngine::quote_identifier("test-api"), r#""test-api""#);
    }

    #[test]
    fn test_qualify_case_sensitive_table() {
        let engine = QueryEngine::new("./data");

        // Case-sensitive table name should be quoted in output
        let sql = r#"SELECT * FROM test-api."TestDiskUsage" LIMIT 2"#;
        let result = engine.qualify_table_names(sql, "_system");
        assert_eq!(
            result,
            r#"SELECT * FROM _system."test-api"."TestDiskUsage" LIMIT 2"#
        );

        // Partially qualified with case-sensitive table
        let sql2 = r#"SELECT * FROM "TestDiskUsage""#;
        let result2 = engine.qualify_table_names(sql2, "_system");
        assert_eq!(result2, r#"SELECT * FROM _system.default."TestDiskUsage""#);
    }

    #[test]
    fn test_extract_table_references() {
        let engine = QueryEngine::new("./data");

        // Simple unqualified table
        let sql = "SELECT * FROM cpu WHERE usage > 50";
        let refs = engine.extract_table_references(sql, "testdb");
        assert_eq!(refs, vec![("testdb".to_string(), "default".to_string(), "cpu".to_string())]);

        // Partially qualified
        let sql = "SELECT * FROM webapp.cpu LIMIT 10";
        let refs = engine.extract_table_references(sql, "testdb");
        assert_eq!(refs, vec![("testdb".to_string(), "webapp".to_string(), "cpu".to_string())]);

        // Fully qualified
        let sql = "SELECT * FROM testdb.webapp.cpu WHERE timestamp > 0";
        let refs = engine.extract_table_references(sql, "testdb");
        assert_eq!(refs, vec![("testdb".to_string(), "webapp".to_string(), "cpu".to_string())]);

        // Multiple tables (JOIN)
        let sql = "SELECT * FROM cpu JOIN webapp.memory ON cpu.hostname = memory.hostname";
        let mut refs = engine.extract_table_references(sql, "testdb");
        refs.sort();
        assert_eq!(refs, vec![
            ("testdb".to_string(), "default".to_string(), "cpu".to_string()),
            ("testdb".to_string(), "webapp".to_string(), "memory".to_string())
        ]);

        // Multiple tables with different qualification levels
        let sql = "SELECT * FROM cpu, otherdb.webapp.disk, service.memory";
        let mut refs = engine.extract_table_references(sql, "testdb");
        refs.sort();
        assert_eq!(refs, vec![
            ("otherdb".to_string(), "webapp".to_string(), "disk".to_string()),
            ("testdb".to_string(), "default".to_string(), "cpu".to_string()),
            ("testdb".to_string(), "service".to_string(), "memory".to_string())
        ]);

        // With quoted identifiers
        let sql = r#"SELECT * FROM default."cat.transaction""#;
        let refs = engine.extract_table_references(sql, "testdb");
        assert_eq!(refs, vec![("testdb".to_string(), "default".to_string(), "cat.transaction".to_string())]);
    }
}
