use serde::{Deserialize, Serialize};
use std::fs::{create_dir_all, read_to_string, write};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub name: String,
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableConfig {
    pub name: String,
    pub partition_cols: Vec<String>,
}

pub struct DatabaseManager {
    data_dir: String,
}

impl DatabaseManager {
    pub fn new(data_dir: &str) -> Self {
        DatabaseManager {
            data_dir: data_dir.to_string(),
        }
    }

    /// Create a new database
    pub fn create_database(&self, db_name: &str) -> Result<String, String> {
        let db_path = format!("{}/{}", self.data_dir, db_name);

        // Check if database already exists
        if Path::new(&db_path).exists() {
            return Err(format!("Database '{}' already exists", db_name));
        }

        // Create directory
        create_dir_all(&db_path)
            .map_err(|e| format!("Failed to create database directory: {}", e))?;

        // Create config file
        let config = DatabaseConfig {
            name: db_name.to_string(),
            tables: Vec::new(),
        };

        self.save_config(&config)?;

        Ok(format!("Database '{}' created at {}", db_name, db_path))
    }

    /// Create a table with optional partitioning
    pub fn create_table(
        &self,
        db_name: &str,
        table_name: &str,
        partition_cols: Vec<String>,
    ) -> Result<String, String> {
        let db_path = format!("{}/{}", self.data_dir, db_name);

        // Check if database exists
        if !Path::new(&db_path).exists() {
            return Err(format!("Database '{}' does not exist", db_name));
        }

        // Load or create config
        let mut config = self.load_config(db_name).unwrap_or_else(|_| DatabaseConfig {
            name: db_name.to_string(),
            tables: Vec::new(),
        });

        // Check if table already exists
        if config.tables.iter().any(|t| t.name == table_name) {
            return Err(format!("Table '{}' already exists", table_name));
        }

        // Add table config
        config.tables.push(TableConfig {
            name: table_name.to_string(),
            partition_cols: partition_cols.clone(),
        });

        self.save_config(&config)?;

        let partition_info = if partition_cols.is_empty() {
            "no partitioning".to_string()
        } else {
            format!("partitioned by: {}", partition_cols.join(", "))
        };

        Ok(format!(
            "Table '{}/{}' created ({})",
            db_name, table_name, partition_info
        ))
    }

    /// List all databases
    pub fn list_databases(&self) -> Result<Vec<String>, String> {
        let entries = std::fs::read_dir(&self.data_dir)
            .map_err(|e| format!("Failed to read data directory: {}", e))?;

        let mut databases = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
            if entry.path().is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    databases.push(name.to_string());
                }
            }
        }

        databases.sort();
        Ok(databases)
    }

    /// List tables in a database
    pub fn list_tables(&self, db_name: &str) -> Result<Vec<TableConfig>, String> {
        let config = self.load_config(db_name)?;
        Ok(config.tables)
    }

    /// Get table configuration
    pub fn get_table_config(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<TableConfig, String> {
        let config = self.load_config(db_name)?;
        config
            .tables
            .into_iter()
            .find(|t| t.name == table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))
    }

    /// Delete a database
    pub fn delete_database(&self, db_name: &str) -> Result<String, String> {
        let db_path = format!("{}/{}", self.data_dir, db_name);

        if !Path::new(&db_path).exists() {
            return Err(format!("Database '{}' does not exist", db_name));
        }

        std::fs::remove_dir_all(&db_path)
            .map_err(|e| format!("Failed to delete database: {}", e))?;

        Ok(format!("Database '{}' deleted", db_name))
    }

    /// Delete a table
    pub fn delete_table(&self, db_name: &str, table_name: &str) -> Result<String, String> {
        let mut config = self.load_config(db_name)?;

        let initial_len = config.tables.len();
        config.tables.retain(|t| t.name != table_name);

        if config.tables.len() == initial_len {
            return Err(format!("Table '{}' not found", table_name));
        }

        self.save_config(&config)?;

        Ok(format!("Table '{}' deleted from database '{}'", table_name, db_name))
    }

    /// Load database config
    fn load_config(&self, db_name: &str) -> Result<DatabaseConfig, String> {
        let config_path = format!("{}/{}/config.json", self.data_dir, db_name);

        if !Path::new(&config_path).exists() {
            return Err(format!("Database config not found for '{}'", db_name));
        }

        let content = read_to_string(&config_path)
            .map_err(|e| format!("Failed to read config: {}", e))?;

        serde_json::from_str(&content).map_err(|e| format!("Failed to parse config: {}", e))
    }

    /// Save database config
    fn save_config(&self, config: &DatabaseConfig) -> Result<(), String> {
        let config_path = format!("{}/{}/config.json", self.data_dir, config.name);

        let content = serde_json::to_string_pretty(config)
            .map_err(|e| format!("Failed to serialize config: {}", e))?;

        write(&config_path, content).map_err(|e| format!("Failed to write config: {}", e))?;

        Ok(())
    }

    /// Get database info
    pub fn get_database_info(&self, db_name: &str) -> Result<DatabaseConfig, String> {
        self.load_config(db_name)
    }
}