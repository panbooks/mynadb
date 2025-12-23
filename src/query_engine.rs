use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct QueryEngine {
    ctx: SessionContext,
    data_dir: String,
    // Track registered databases (schemas)
    registered_databases: Arc<RwLock<HashMap<String, bool>>>,
}

impl QueryEngine {
    pub fn new(data_dir: &str) -> Self {
        let ctx = SessionContext::new();
        QueryEngine {
            ctx,
            data_dir: data_dir.to_string(),
            registered_databases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a database tables in DataFusion catalog
    pub async fn ensure_database_registered(&self, db_name: &str) -> Result<(), String> {
        // Check if already registered
        {
            let registered = self.registered_databases.read().await;
            if registered.contains_key(db_name) {
                return Ok(());
            }
        }

        let db_path = format!("{}/{}", self.data_dir, db_name);

        // Check if database directory exists
        if !std::path::Path::new(&db_path).exists() {
            return Err(format!("Database '{}' not found", db_name));
        }

        // Read database directory to find measurement directories
        let entries = std::fs::read_dir(&db_path)
            .map_err(|e| format!("Failed to read directory: {}", e))?;

        let mut registered_count = 0;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            // Check if it's a directory (measurement directory)
            if path.is_dir() {
                if let Some(measurement) = path.file_name().and_then(|s| s.to_str()) {
                    // Skip hidden directories and special files
                    if measurement.starts_with('.') {
                        continue;
                    }

                    // Register with database prefix (e.g., "mydb_cpu")
                    // Note: We use underscore for registration, but support dot notation in queries
                    let table_name = format!("{}_{}", db_name, measurement);
                    let measurement_path = path.to_str().unwrap();

                    self.ctx
                        .register_parquet(&table_name, measurement_path, ParquetReadOptions::default())
                        .await
                        .map_err(|e| format!("Failed to register table '{}': {}", table_name, e))?;

                    registered_count += 1;
                }
            }
        }

        // Mark database as registered
        {
            let mut registered = self.registered_databases.write().await;
            registered.insert(db_name.to_string(), true);
        }

        if registered_count > 0 {
            println!("📋 Registered {} table(s) in database '{}'", registered_count, db_name);
        }

        Ok(())
    }

    /// Register all Parquet files in a database as tables
    pub async fn register_database(&self, db_name: &str) -> Result<Vec<String>, String> {
        self.ensure_database_registered(db_name).await?;

        // List all registered tables for this database
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table_names = schema.table_names();

        let prefix = format!("{}_", db_name);
        let tables: Vec<String> = table_names
            .into_iter()
            .filter(|t| t.starts_with(&prefix))
            .collect();

        Ok(tables)
    }

    /// Execute a SQL query with database context
    /// Automatically prefixes unqualified table names with the database name
    pub async fn execute_query_with_db(&self, sql: &str, db_name: &str) -> Result<Vec<RecordBatch>, String> {
        // Prefix unqualified table names with "database_"
        let prefixed_sql = self.prefix_table_names(sql, db_name);
        
        println!("📝 SQL: {}", sql);
        if prefixed_sql != sql {
            println!("📝 Prefixed: {}", prefixed_sql);
        }
        
        let df = self
            .ctx
            .sql(&prefixed_sql)
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

    /// Prefix unqualified table names with database name
    /// Example: "SELECT * FROM cpu" -> "SELECT * FROM mydb_cpu"
    fn prefix_table_names(&self, sql: &str, db_name: &str) -> String {
        // Simple approach: replace table references after FROM/JOIN keywords
        let sql_upper = sql.to_uppercase();
        let mut result = sql.to_string();
        
        let keywords = ["FROM", "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"];
        
        for keyword in &keywords {
            let pattern = format!("{} ", keyword);
            let mut search_pos = 0;
            
            while let Some(pos) = sql_upper[search_pos..].find(&pattern) {
                let abs_pos = search_pos + pos + pattern.len();
                let remaining = &sql[abs_pos..];
                
                // Find end of table name
                let end_pos = remaining
                    .find(|c: char| c.is_whitespace() || c == ',' || c == ';' || c == ')')
                    .unwrap_or(remaining.len());
                
                let table_ref = remaining[..end_pos].trim();
                
                // Only prefix if:
                // 1. Not empty
                // 2. Doesn't already start with database prefix
                // 3. Isn't a subquery
                if !table_ref.is_empty() 
                    && !table_ref.starts_with(&format!("{}_", db_name))
                    && !table_ref.contains('(')
                {
                    let prefixed = format!("{}_{}", db_name, table_ref);
                    result = result.replacen(table_ref, &prefixed, 1);
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
        use datafusion::arrow::datatypes::DataType;

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
            _ => Ok(serde_json::json!(format!("Unsupported type: {:?}", array.data_type()))),
        }
    }

    /// List all registered tables
    pub async fn list_tables(&self) -> Vec<String> {
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let mut tables = schema.table_names();
        tables.sort();
        tables
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

    /// Clear registration cache (useful after flush)
    pub async fn clear_cache(&self) {
        let mut registered = self.registered_databases.write().await;
        registered.clear();
        println!("🔄 Query engine cache cleared");
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
}
