use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct QueryEngine {
    ctx: SessionContext,
    data_dir: String,
    // Cache of registered table names
    registered_tables: Arc<Mutex<HashSet<String>>>,
}

impl QueryEngine {
    pub fn new(data_dir: &str) -> Self {
        let ctx = SessionContext::new();
        QueryEngine {
            ctx,
            data_dir: data_dir.to_string(),
            registered_tables: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Auto-register database tables (simplified - no manual registration needed)
    pub async fn ensure_database_registered(&self, db_name: &str) -> Result<(), String> {
        let db_path = format!("{}/{}", self.data_dir, db_name);

        // Check if database directory exists
        if !std::path::Path::new(&db_path).exists() {
            return Err(format!("Database '{}' not found", db_name));
        }

        // Read database directory to find measurement directories
        let entries = std::fs::read_dir(&db_path)
            .map_err(|e| format!("Failed to read directory: {}", e))?;

        let mut registered_count = 0;
        let mut registered_tables = self.registered_tables.lock().await;

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

                    let table_name = format!("{}_{}", db_name, measurement);

                    // Only register if not already registered
                    if !registered_tables.contains(&table_name) {
                        // Register the entire measurement directory as a table
                        let measurement_path = path.to_str().unwrap();

                        self.ctx
                            .register_parquet(&table_name, measurement_path, ParquetReadOptions::default())
                            .await
                            .map_err(|e| format!("Failed to register table '{}': {}", table_name, e))?;

                        registered_tables.insert(table_name.clone());
                        registered_count += 1;
                    }
                }
            }
        }

        if registered_count > 0 {
            println!("📋 Registered {} new table(s) in database '{}'", registered_count, db_name);
        }

        Ok(())
    }

    /// Register all Parquet files in a database as tables (legacy method for compatibility)
    pub async fn register_database(&self, db_name: &str) -> Result<Vec<String>, String> {
        self.ensure_database_registered(db_name).await?;

        let registered = self.registered_tables.lock().await;
        let tables: Vec<String> = registered
            .iter()
            .filter(|t| t.starts_with(&format!("{}_", db_name)))
            .cloned()
            .collect();

        Ok(tables)
    }

    /// Execute a SQL query
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
        let registered = self.registered_tables.lock().await;
        let mut tables: Vec<_> = registered.iter().cloned().collect();
        tables.sort();
        tables
    }

    /// Get schema for a table
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
        let mut registered = self.registered_tables.lock().await;
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
