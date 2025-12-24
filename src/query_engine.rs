use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::*;
use datafusion::catalog_common::MemoryCatalogProvider;
use datafusion::catalog_common::MemorySchemaProvider;
use datafusion::sql::TableReference;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn, debug};

const DEFAULT_SCHEMA: &str = "default";

pub struct QueryEngine {
    ctx: SessionContext,
    data_dir: String,
    // Track registered catalogs (databases) and their schemas (services)
    registered_catalogs: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl QueryEngine {
    pub fn new(data_dir: &str) -> Self {
        let ctx = SessionContext::new();
        QueryEngine {
            ctx,
            data_dir: data_dir.to_string(),
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
    async fn ensure_schema_in_catalog(&self, catalog_name: &str, schema_name: &str) -> Result<(), String> {
        self.ensure_catalog(catalog_name).await?;

        let catalog = self.ctx.catalog(catalog_name)
            .ok_or_else(|| format!("Catalog '{}' not found", catalog_name))?;

        if catalog.schema(schema_name).is_none() {
            let schema = Arc::new(MemorySchemaProvider::new());
            catalog.register_schema(schema_name, schema)
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
        // Build path: data_dir/catalog/schema/table/
        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog_name, schema_name, table_name);

        if !std::path::Path::new(&table_path).exists() {
            return Err(format!("Table path not found: {}", table_path));
        }

        // Ensure catalog and schema exist
        self.ensure_schema_in_catalog(catalog_name, schema_name).await?;

        // Check if table is already registered
        if let Some(catalog) = self.ctx.catalog(catalog_name) {
            if let Some(schema) = catalog.schema(schema_name) {
                if schema.table_exist(table_name) {
                    // Table already registered, skip
                    return Ok(());
                }
            }
        }

        // Check if table has Hive-style partitions
        let partitions = self.discover_table_partitions(&table_path)?;
        
        if !partitions.is_empty() {
            // Use CREATE EXTERNAL TABLE with PARTITIONED BY for Hive partitions
            let create_sql = format!(
                "CREATE EXTERNAL TABLE {}.{}.{} \
                 STORED AS PARQUET \
                 PARTITIONED BY (date) \
                 LOCATION '{}'",
                catalog_name, schema_name, table_name, table_path
            );
            
            self.ctx.sql(&create_sql).await
                .map_err(|e| format!("Failed to create partitioned table '{}.{}.{}': {}", catalog_name, schema_name, table_name, e))?;
            
            debug!("Registered partitioned table: {}.{}.{} with {} partitions", 
                   catalog_name, schema_name, table_name, partitions.len());
        } else {
            // No partitions, use standard registration
            let table_ref = TableReference::full(catalog_name, schema_name, table_name);
            self.ctx
                .register_parquet(table_ref, &table_path, ParquetReadOptions::default())
                .await
                .map_err(|e| format!("Failed to register table '{}.{}.{}': {}", catalog_name, schema_name, table_name, e))?;
            
            debug!("Registered non-partitioned table: {}.{}.{}", catalog_name, schema_name, table_name);
        }

        // Track registration
        let mut catalogs = self.registered_catalogs.write().await;
        catalogs.entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());

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

    /// List all partitions for a specific table
    pub async fn list_table_partitions(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Vec<String>, String> {
        let table_path = format!("{}/{}/{}/{}", self.data_dir, catalog_name, schema_name, table_name);
        self.discover_table_partitions(&table_path)
    }

    /// Auto-discover and register all tables in catalog.schema from disk
    pub async fn discover_and_register(&self, catalog_name: &str, schema_name: &str) -> Result<usize, String> {
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
                        if let Err(e) = self.register_table(catalog_name, schema_name, table_name).await {
                            warn!("Failed to register {}.{}.{}: {}", catalog_name, schema_name, table_name, e);
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
                            Err(e) => warn!("Failed to discover schema {}.{}: {}", catalog_name, schema_name, e),
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
        catalogs.entry(catalog_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(schema_name.to_string());
        Ok(())
    }

    /// Legacy compatibility: ensure database registered (now auto-discovers all schemas)
    pub async fn ensure_database_registered(&self, db_name: &str) -> Result<(), String> {
        let count = self.discover_catalog(db_name).await?;
        if count > 0 {
            info!("Auto-registered {} table(s) in catalog '{}'", count, db_name);
        }
        Ok(())
    }

    /// Register all Parquet files in a database as tables
    pub async fn register_database(&self, db_name: &str) -> Result<Vec<String>, String> {
        self.ensure_database_registered(db_name).await?;

        // List all schemas in this catalog
        let schemas = self.list_schemas(db_name).await?;
        let mut all_tables = Vec::new();

        for schema_name in schemas {
            let tables = self.list_tables_in_schema(db_name, &schema_name).await?;
            for table in tables {
                all_tables.push(format!("{}.{}.{}", db_name, schema_name, table));
            }
        }

        Ok(all_tables)
    }

    /// Execute a SQL query with database context
    /// Automatically qualifies unqualified table names with catalog.schema
    pub async fn execute_query_with_db(&self, sql: &str, db_name: &str) -> Result<Vec<RecordBatch>, String> {
        // Qualify unqualified table names with catalog.schema
        let qualified_sql = self.qualify_table_names(sql, db_name);

        debug!("SQL: {}", sql);
        if qualified_sql != sql {
            debug!("Qualified: {}", qualified_sql);
        }

        let df = self
            .ctx
            .sql(&qualified_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Execution error: {}", e))?;

        Ok(batches)
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
    pub async fn get_query_schema(&self, db_name: &str, sql: &str) -> Result<Arc<datafusion::arrow::datatypes::Schema>, String> {
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

    /// Qualify unqualified or partially qualified table names with catalog.schema
    ///
    /// Examples (with catalog_name = "testdb"):
    /// - "cpu" -> "testdb.default.cpu"
    /// - "webapp.cpu" -> "testdb.webapp.cpu"
    /// - "testdb.webapp.cpu" -> "testdb.webapp.cpu" (already fully qualified)
    /// - "otherdb.schema.table" -> "otherdb.schema.table" (different catalog, unchanged)
    fn qualify_table_names(&self, sql: &str, catalog_name: &str) -> String {
        let sql_upper = sql.to_uppercase();
        let mut result = sql.to_string();

        let keywords = ["FROM", "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"];

        for keyword in &keywords {
            let pattern = format!("{} ", keyword);
            let mut search_pos = 0;

            while let Some(pos) = sql_upper[search_pos..].find(&pattern) {
                let abs_pos = search_pos + pos + pattern.len();
                let remaining = &sql[abs_pos..];

                // Find end of table reference
                let end_pos = remaining
                    .find(|c: char| c.is_whitespace() || c == ',' || c == ';' || c == ')')
                    .unwrap_or(remaining.len());

                let table_ref = remaining[..end_pos].trim();

                if table_ref.is_empty() || table_ref.contains('(') {
                    search_pos = abs_pos + end_pos;
                    continue;
                }

                // Count dots to determine qualification level
                let dot_count = table_ref.matches('.').count();

                let qualified = if dot_count == 0 {
                    // Simple name: cpu -> catalog.default.cpu
                    format!("{}.{}.{}", catalog_name, DEFAULT_SCHEMA, table_ref)
                } else if dot_count == 1 {
                    // Partially qualified: webapp.cpu -> catalog.webapp.cpu
                    format!("{}.{}", catalog_name, table_ref)
                } else {
                    // Already fully qualified (catalog.schema.table), don't modify
                    table_ref.to_string()
                };

                if qualified != table_ref {
                    result = result.replacen(table_ref, &qualified, 1);
                }

                search_pos = abs_pos + end_pos;
            }
        }

        result
    }

    /// Execute query and return as JSON (with DB context)
    pub async fn execute_query_json_with_db(&self, sql: &str, db_name: &str) -> Result<String, String> {
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

    /// Execute query and return as JSON
    pub async fn execute_query_json(&self, sql: &str) -> Result<String, String> {
        let batches = self.execute_query(sql).await?;

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
                        let dict_array = array.as_any().downcast_ref::<DictionaryArray<UInt16Type>>().unwrap();
                        let keys = dict_array.keys();
                        let key = keys.value(index);
                        let values = dict_array.values().as_any().downcast_ref::<StringArray>().unwrap();
                        Ok(serde_json::json!(values.value(key as usize)))
                    }
                    _ => Ok(serde_json::json!(format!("Unsupported dictionary value type: {:?}", value_type))),
                }
            }
            _ => Ok(serde_json::json!(format!("Unsupported type: {:?}", array.data_type()))),
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
                            all_tables.push(format!("{}.{}.{}", catalog_name, schema_name, table_name));
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
    pub async fn list_tables_in_schema(&self, catalog_name: &str, schema_name: &str) -> Result<Vec<String>, String> {
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

    /// Clear registration cache (useful after flush)
    pub async fn clear_cache(&self) {
        let mut catalogs = self.registered_catalogs.write().await;
        catalogs.clear();
        info!("Query engine cache cleared");
    }

    /// Get the session context (for Flight service)
    pub fn get_context(&self) -> &SessionContext {
        &self.ctx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_query_engine() {
        let engine = QueryEngine::new("./data");
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
        assert_eq!(result, "SELECT * FROM testdb.webapp.cpu WHERE timestamp > 0");
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
    fn test_qualify_table_names_join_variations() {
        let engine = QueryEngine::new("./data");

        // Test different JOIN types
        let sql = "SELECT * FROM cpu INNER JOIN memory LEFT JOIN disk RIGHT JOIN network";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT * FROM testdb.default.cpu INNER JOIN testdb.default.memory LEFT JOIN testdb.default.disk RIGHT JOIN testdb.default.network");
    }

    #[test]
    fn test_qualify_table_names_with_alias() {
        let engine = QueryEngine::new("./data");

        // Table with alias
        let sql = "SELECT c.* FROM cpu c WHERE c.usage > 50";
        let result = engine.qualify_table_names(sql, "testdb");
        assert_eq!(result, "SELECT c.* FROM testdb.default.cpu c WHERE c.usage > 50");
    }

    #[test]
    fn test_qualify_table_names_complex_query() {
        let engine = QueryEngine::new("./data");

        // Complex query with mixed qualification levels
        let sql = r#"
            SELECT a.*, b.value
            FROM cpu a
            INNER JOIN webapp.memory b ON a.hostname = b.hostname
            LEFT JOIN testdb.api.disk c ON a.hostname = c.hostname
            WHERE a.usage > 50
        "#;
        let result = engine.qualify_table_names(sql, "testdb");
        assert!(result.contains("FROM testdb.default.cpu a"));
        assert!(result.contains("JOIN testdb.webapp.memory b"));
        assert!(result.contains("JOIN testdb.api.disk c"));
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

        // Subqueries should not be modified (contains parentheses)
        let sql = "SELECT * FROM (SELECT * FROM cpu) AS subquery";
        let result = engine.qualify_table_names(sql, "testdb");
        // The outer FROM has a subquery (with parenthesis), so it's skipped
        // The inner FROM should be qualified
        assert!(result.contains("FROM cpu"));
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
}
