use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;

pub struct QueryEngine {
    ctx: SessionContext,
    data_dir: String,
}

impl QueryEngine {
    pub fn new(data_dir: &str) -> Self {
        let ctx = SessionContext::new();
        QueryEngine {
            ctx,
            data_dir: data_dir.to_string(),
        }
    }

    /// Register all Parquet files in a database as tables
    pub async fn register_database(&self, db_name: &str) -> Result<Vec<String>, String> {
        let db_path = format!("{}/{}", self.data_dir, db_name);

        // Check if database directory exists
        if !std::path::Path::new(&db_path).exists() {
            return Err(format!("Database '{}' not found", db_name));
        }

        let mut registered_tables = Vec::new();

        // Read directory and find all parquet files
        let entries = std::fs::read_dir(&db_path)
            .map_err(|e| format!("Failed to read directory: {}", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                // Extract measurement name from filename
                // Format: measurement_timestamp.parquet
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    let measurement = filename
                        .rsplitn(2, '_')
                        .nth(1)
                        .unwrap_or(filename);

                    // Register as table (use measurement name)
                    let table_name = format!("{}_{}", db_name, measurement);

                    self.ctx
                        .register_parquet(&table_name, path.to_str().unwrap(), ParquetReadOptions::default())
                        .await
                        .map_err(|e| format!("Failed to register table: {}", e))?;

                    registered_tables.push(table_name);
                }
            }
        }

        Ok(registered_tables)
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
    pub fn list_tables(&self) -> Vec<String> {
        self.ctx
            .catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table_names()
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