//! Storage for Continuous Aggregate Definitions
//!
//! This module handles persistence of aggregate definitions and watermarks.
//! Definitions are stored as JSON files in a dedicated directory.
//!
//! In cluster mode, this can be extended to store in the Meta service
//! for cluster-wide consistency.

use super::definition::AggregateDefinition;
use super::types::{AggregateState, WatermarkInfo};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// File extension for aggregate definition files
const DEFINITION_EXT: &str = ".json";

/// Subdirectory for watermark files
const WATERMARKS_DIR: &str = "watermarks";

/// Storage backend for aggregate definitions
pub struct AggregateStorage {
    /// Base directory for storing definitions
    base_dir: PathBuf,

    /// In-memory cache of definitions
    cache: Arc<RwLock<HashMap<String, AggregateDefinition>>>,

    /// Whether the cache has been initialized
    initialized: Arc<RwLock<bool>>,
}

impl AggregateStorage {
    /// Create a new storage backend
    pub fn new(data_dir: &str) -> Self {
        let layout = crate::storage::layout::DataLayout::new(data_dir);
        let base_dir = layout.aggregates_dir();
        Self {
            base_dir,
            cache: Arc::new(RwLock::new(HashMap::new())),
            initialized: Arc::new(RwLock::new(false)),
        }
    }

    /// Create a storage backend with custom directory
    pub fn with_dir(dir: PathBuf) -> Self {
        Self {
            base_dir: dir,
            cache: Arc::new(RwLock::new(HashMap::new())),
            initialized: Arc::new(RwLock::new(false)),
        }
    }

    /// Ensure the storage directory exists
    fn ensure_dirs(&self) -> Result<(), String> {
        std::fs::create_dir_all(&self.base_dir).map_err(|e| {
            format!(
                "Failed to create aggregate storage directory {}: {}",
                self.base_dir.display(),
                e
            )
        })?;

        let watermarks_dir = self.base_dir.join(WATERMARKS_DIR);
        std::fs::create_dir_all(&watermarks_dir).map_err(|e| {
            format!(
                "Failed to create watermarks directory {}: {}",
                watermarks_dir.display(),
                e
            )
        })?;

        Ok(())
    }

    /// Get the file path for a definition
    fn definition_path(&self, name: &str) -> PathBuf {
        self.base_dir.join(format!("{}{}", name, DEFINITION_EXT))
    }

    /// Get the file path for a watermark
    fn watermark_path(&self, name: &str) -> PathBuf {
        self.base_dir
            .join(WATERMARKS_DIR)
            .join(format!("{}{}", name, DEFINITION_EXT))
    }

    /// Initialize the cache by loading all definitions from disk
    pub async fn initialize(&self) -> Result<(), String> {
        let mut initialized = self.initialized.write().await;
        if *initialized {
            return Ok(());
        }

        self.ensure_dirs()?;
        self.load_all_definitions().await?;
        *initialized = true;

        Ok(())
    }

    /// Load all definitions from disk into cache
    async fn load_all_definitions(&self) -> Result<(), String> {
        let entries = std::fs::read_dir(&self.base_dir).map_err(|e| {
            format!(
                "Failed to read aggregate directory {}: {}",
                self.base_dir.display(),
                e
            )
        })?;

        let mut cache = self.cache.write().await;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().map(|e| e == "json").unwrap_or(false) {
                if let Some(stem) = path.file_stem() {
                    let name = stem.to_string_lossy().to_string();
                    match self.load_definition_from_file(&path) {
                        Ok(mut def) => {
                            // Load watermark if exists
                            if let Ok(wm) = self.load_watermark_from_file(&name) {
                                def.watermark = wm;
                            }
                            debug!("Loaded aggregate definition: {}", name);
                            cache.insert(name, def);
                        }
                        Err(e) => {
                            warn!(
                                "Failed to load aggregate definition {}: {}",
                                path.display(),
                                e
                            );
                        }
                    }
                }
            }
        }

        info!("Loaded {} aggregate definitions", cache.len());
        Ok(())
    }

    /// Load a definition from a file
    fn load_definition_from_file(&self, path: &Path) -> Result<AggregateDefinition, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;

        serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse {}: {}", path.display(), e))
    }

    /// Load a watermark from file
    fn load_watermark_from_file(&self, name: &str) -> Result<WatermarkInfo, String> {
        let path = self.watermark_path(name);
        let content = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read watermark {}: {}", path.display(), e))?;

        serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse watermark {}: {}", path.display(), e))
    }

    /// Save a definition to disk
    pub async fn save_definition(&self, definition: &AggregateDefinition) -> Result<(), String> {
        self.ensure_dirs()?;

        let path = self.definition_path(&definition.name);
        let content = serde_json::to_string_pretty(definition)
            .map_err(|e| format!("Failed to serialize definition: {}", e))?;

        std::fs::write(&path, content)
            .map_err(|e| format!("Failed to write {}: {}", path.display(), e))?;

        // Update cache
        let mut cache = self.cache.write().await;
        cache.insert(definition.name.clone(), definition.clone());

        info!("Saved aggregate definition: {}", definition.name);
        Ok(())
    }

    /// Save a watermark to disk
    pub async fn save_watermark(&self, watermark: &WatermarkInfo) -> Result<(), String> {
        self.ensure_dirs()?;

        let path = self.watermark_path(&watermark.aggregate_name);
        let content = serde_json::to_string_pretty(watermark)
            .map_err(|e| format!("Failed to serialize watermark: {}", e))?;

        std::fs::write(&path, content)
            .map_err(|e| format!("Failed to write {}: {}", path.display(), e))?;

        // Update cache
        let mut cache = self.cache.write().await;
        if let Some(def) = cache.get_mut(&watermark.aggregate_name) {
            def.watermark = watermark.clone();
        }

        debug!(
            "Saved watermark for {}: high={}",
            watermark.aggregate_name, watermark.high_watermark_ns
        );
        Ok(())
    }

    /// Delete a definition
    pub async fn delete_definition(&self, name: &str) -> Result<(), String> {
        let path = self.definition_path(name);
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| format!("Failed to delete {}: {}", path.display(), e))?;
        }

        // Also delete watermark
        let wm_path = self.watermark_path(name);
        if wm_path.exists() {
            let _ = std::fs::remove_file(&wm_path);
        }

        // Update cache
        let mut cache = self.cache.write().await;
        cache.remove(name);

        info!("Deleted aggregate definition: {}", name);
        Ok(())
    }

    /// Get a definition by name
    pub async fn get_definition(&self, name: &str) -> Result<Option<AggregateDefinition>, String> {
        // Initialize if needed
        let initialized = self.initialized.read().await;
        if !*initialized {
            drop(initialized);
            self.initialize().await?;
        }

        let cache = self.cache.read().await;
        Ok(cache.get(name).cloned())
    }

    /// List all definitions
    pub async fn list_definitions(&self) -> Result<Vec<AggregateDefinition>, String> {
        // Initialize if needed
        let initialized = self.initialized.read().await;
        if !*initialized {
            drop(initialized);
            self.initialize().await?;
        }

        let cache = self.cache.read().await;
        Ok(cache.values().cloned().collect())
    }

    /// Get all definitions that need refresh
    pub async fn get_pending_refreshes(&self) -> Result<Vec<AggregateDefinition>, String> {
        let definitions = self.list_definitions().await?;

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);

        let pending: Vec<_> = definitions
            .into_iter()
            .filter(|def| {
                // Only active aggregates
                if def.state != AggregateState::Active && def.state != AggregateState::Creating {
                    return false;
                }

                // Check if refresh interval has passed
                let last_refresh = def.watermark.last_refresh_at;
                let interval_ns = def.refresh_policy.interval_secs as i64 * 1_000_000_000;
                now_ns - last_refresh >= interval_ns
            })
            .collect();

        Ok(pending)
    }

    /// Update the state of an aggregate
    pub async fn update_state(&self, name: &str, state: AggregateState) -> Result<(), String> {
        let mut cache = self.cache.write().await;
        if let Some(def) = cache.get_mut(name) {
            def.state = state;
            def.modified_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0);

            // Write to disk
            let path = self.definition_path(name);
            let content = serde_json::to_string_pretty(&*def)
                .map_err(|e| format!("Failed to serialize: {}", e))?;
            std::fs::write(&path, content).map_err(|e| format!("Failed to write: {}", e))?;

            debug!("Updated aggregate {} state to {:?}", name, state);
            Ok(())
        } else {
            Err(format!("Aggregate not found: {}", name))
        }
    }

    /// Find aggregates that can answer a query on the given source table
    pub async fn find_matching_aggregates(
        &self,
        source_table: &str,
        time_bucket_nanos: Option<i64>,
    ) -> Result<Vec<AggregateDefinition>, String> {
        let definitions = self.list_definitions().await?;

        let matches: Vec<_> = definitions
            .into_iter()
            .filter(|def| {
                // Must be active
                if def.state != AggregateState::Active {
                    return false;
                }

                // Must match source table
                if def.source_table != source_table {
                    return false;
                }

                // If time bucket specified, aggregate must be able to answer it
                if let Some(bucket) = time_bucket_nanos {
                    if !def.can_answer_query(bucket) {
                        return false;
                    }
                }

                true
            })
            .collect();

        Ok(matches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregates::definition::AggregateFunction;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_storage_crud() {
        let dir = tempdir().unwrap();
        let storage = AggregateStorage::with_dir(dir.path().to_path_buf());

        // Create a definition
        let def = AggregateDefinition::builder("test_agg", "mydb.default.cpu")
            .time_bucket("time", "1h", "hour")
            .unwrap()
            .avg("value", "avg_value")
            .build()
            .unwrap();

        // Save
        storage.save_definition(&def).await.unwrap();

        // Get
        let loaded = storage.get_definition("test_agg").await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().name, "test_agg");

        // List
        let all = storage.list_definitions().await.unwrap();
        assert_eq!(all.len(), 1);

        // Delete
        storage.delete_definition("test_agg").await.unwrap();
        let loaded = storage.get_definition("test_agg").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_watermark_persistence() {
        let dir = tempdir().unwrap();
        let storage = AggregateStorage::with_dir(dir.path().to_path_buf());

        // Create and save definition
        let def = AggregateDefinition::builder("test_agg", "mydb.default.cpu")
            .time_bucket("time", "1h", "hour")
            .unwrap()
            .avg("value", "avg_value")
            .build()
            .unwrap();
        storage.save_definition(&def).await.unwrap();

        // Update watermark
        let mut wm = WatermarkInfo::new("test_agg");
        wm.update(1000000000, 100, 1024);
        storage.save_watermark(&wm).await.unwrap();

        // Reload and verify watermark is preserved
        let storage2 = AggregateStorage::with_dir(dir.path().to_path_buf());
        storage2.initialize().await.unwrap();
        let loaded = storage2.get_definition("test_agg").await.unwrap().unwrap();
        assert_eq!(loaded.watermark.high_watermark_ns, 1000000000);
        assert_eq!(loaded.watermark.materialized_rows, 100);
    }
}
